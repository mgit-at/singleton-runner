//
// Copyright: 2016, mgIT GmbH <office@mgit.at>
// All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

const (
	LOCK_FILE_PREFIX = "/singleton.mgit.at/"
)

// command line flags
var (
	flagNameTemplate   = flag.String("name-template", "", "template for the lockfile name (will get expanded using environment variables)")
	flagInstTemplate   = flag.String("instance-template", "", "template for the instance name (will get expanded using environment variables)")
	flagRequestTimeout = flag.Uint("request-timeout", 5, "timeout in seconds to wait for responses from etcd")
	flagGracePeriod    = flag.Uint("grace-period", 25, "time in seconds to wait for a normal shutdown of the child")
	flagKillBackoff    = flag.Uint("kill-backoff", 5, "")
	flagEtcd           = flag.String("etcd", "http://127.0.0.1:2379", "etcd machines (comma separated list)")
	flagCA             = flag.String("ca", "", "CA certificate")
	flagCert           = flag.String("cert", "", "client certificate")
	flagKey            = flag.String("key", "", "client key")
)

func runChild(cmd string, args []string) (child exec.Cmd, exited chan bool, err error) {
	child.Path = cmd
	child.Args = args
	child.Stdin = os.Stdin
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr

	if err = child.Start(); err != nil {
		return
	}

	exited = make(chan bool, 1)
	go func() {
		defer func() {
			exited <- true
		}()
		err := child.Wait()
		if err != nil {
			log.Printf("singleton-runner: child exited with: %v", err)
		} else {
			log.Printf("singleton-runner: child exited normally")
		}
	}()

	return
}

func initETCdClient(machines []string, CA, cert, key string, timeout time.Duration) (*etcd.Client, error) {
	var tlsConfig *tls.Config
	if cert != "" && key != "" && CA != "" {
		cert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			log.Fatal("failed to load client key pair:", err)
		}
		caCert, err := ioutil.ReadFile(CA)
		if err != nil {
			log.Fatal("failed to load CA:", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		tlsConfig.BuildNameToCertificate()
	}

	cfg := etcd.Config{
		Endpoints:   machines,
		TLS:         tlsConfig,
		DialTimeout: timeout,
	}

	c, err := etcd.New(cfg)
	if err != nil {
		return nil, err
	}
	return c, err
}

func acquireLock(cli *etcd.Client, lockfile, instanceID string, ttl int64, timeout time.Duration) (etcd.LeaseID, <-chan *etcd.LeaseKeepAliveResponse, context.CancelFunc, error) {
AcquireLoop:
	for {
		log.Printf("singleton-runner: trying to acquire lock: %s (TTL: %v)", lockfile, ttl)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		resp, err := cli.Grant(ctx, ttl)
		if err != nil {
			cancel()
			return etcd.NoLease, nil, nil, err
		}

		log.Printf("singleton-runner: got lease with ID: %v", resp.ID)

		respTxn, err := cli.Txn(ctx).
			If(etcd.Compare(etcd.Version(lockfile), "=", 0)).
			Then(etcd.OpPut(lockfile, instanceID, etcd.WithLease(resp.ID))).Commit()
		cancel()
		if err != nil {
			return etcd.NoLease, nil, nil, err
		}
		if respTxn.Succeeded {
			ctxKA, cancelKA := context.WithCancel(context.Background())
			ch, err := cli.KeepAlive(ctxKA, resp.ID)
			if err != nil {
				return etcd.NoLease, nil, nil, err
			}
			return resp.ID, ch, cancelKA, nil
		}

		log.Printf("singleton-runner: lock is already acquired - watching for changes")
		ctxWatch, cancelWatch := context.WithTimeout(context.Background(), 3*time.Duration(ttl)*time.Second) // TODO: how long should we wait for this?
		watch := cli.Watch(ctxWatch, lockfile)
		for resp := range watch {
			if len(resp.Events) > 0 {
				lastEvent := resp.Events[len(resp.Events)-1]
				if lastEvent.Type == etcd.EventTypeDelete {
					log.Printf("singleton-runner: lock just got deleted...")
					cancelWatch()
					continue AcquireLoop
				}
			}
		}
		cancelWatch()
		return etcd.NoLease, nil, nil, errors.New("watching for events failed/timed-out")
	}
}

func releaseLock(lease etcd.Lease, id etcd.LeaseID, ttl int64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ttl)*time.Second)
	_, err := lease.Revoke(ctx, id)
	cancel()
	if err != nil {
		log.Printf("singleton-runner: releasing lock failed: %s", err)
	}
	return
}

func main() {
	//
	// **** parse command line
	//
	flag.Parse()

	if *flagNameTemplate == "" {
		log.Fatal("singleton-runner: '-name-template' is empty")
	}
	name := os.ExpandEnv(*flagNameTemplate)
	lockfilePath := LOCK_FILE_PREFIX + name

	if *flagInstTemplate == "" {
		log.Fatal("singleton-runner: '-instance-template' is empty")
	}
	instanceID := os.ExpandEnv(*flagInstTemplate)

	requestTimeout := time.Duration(*flagRequestTimeout) * time.Second
	gracePeriod := time.Duration(*flagGracePeriod) * time.Second
	killBackoff := time.Duration(*flagKillBackoff) * time.Second
	// etcd library sends keep alives after ttl/3 seconds ...
	// to make sure we have enough time to cleanup:
	//
	//     ttl = rt + ttl/3 + gp + kb   --->   ttl = 3/2 * (rt + gp + kb)
	//
	//   add 1 second because these are integers and we want to round up in any case
	ttl := int64((3*(*flagRequestTimeout+*flagGracePeriod+*flagKillBackoff))/2) + 1

	machines := strings.Split(*flagEtcd, ",")
	CA := *flagCA
	cert := *flagCert
	key := *flagKey

	var cmd string
	args := flag.Args()
	if args := flag.Args(); len(args) > 0 {
		cmd = args[0]
	} else {
		log.Fatal("singleton-runner: please specify a command to run")
	}

	//
	// **** initialization
	//

	cli, err := initETCdClient(machines, CA, cert, key, requestTimeout)
	if err != nil {
		log.Fatal("error connecting to etcd:", err)
	}
	defer cli.Close()

	//
	// **** try to get the lock
	//
	leaseID, kaCh, kaCancel, err := acquireLock(cli, lockfilePath, instanceID, ttl, requestTimeout)
	if err != nil {
		log.Fatal("singleton-runner: unable to acquire lock: ", err)
	}
	log.Printf("singleton-runner: lock acquired successfully! .. starting %q", cmd)
	defer releaseLock(cli, leaseID, ttl)

	//
	// **** run the child
	//
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)

	child, exited, err := runChild(cmd, args)
	if err != nil {
		log.Fatal("singleton-runner: calling child failed:", err)
	}

	//
	// **** update the lock - release lock if child exits
	//
Loop:
	for {
		select {
		//case resp, ok := <-kaCh:
		case _, ok := <-kaCh:
			if !ok {
				log.Println("singleton-runner: keep alive reponse channel is closed - sending child the TERM signal")
				child.Process.Signal(syscall.SIGTERM)
				break Loop
			}
			// else {
			// 	log.Printf("singleton-runner: got keep alive response: %v", time.Duration(resp.TTL)*time.Second)
			// }
		case <-exited:
			log.Println("singleton-runner: closing...")
			kaCancel()
			return
		case sig := <-signals:
			child.Process.Signal(sig)
			if sig == syscall.SIGTERM || sig == syscall.SIGINT {
				log.Printf("singleton-runner: got signal %v ... waiting %v for child to terminate", sig, gracePeriod)
				break Loop
			}
		}
	}
	kaCancel()

	//
	// **** wait for child to exit - kill it after grace period
	//
	g := time.NewTimer(gracePeriod)
	k := time.NewTimer(gracePeriod + killBackoff)
	for {
		select {
		case <-g.C:
			log.Println("singleton-runner: grace period elapsed sending child the KILL signal")
			child.Process.Signal(syscall.SIGKILL)
		case <-k.C:
			log.Println("singleton-runner: child is still running ... something is very wrong!!!")
			signal.Reset()
			return
		case <-exited:
			log.Println("singleton-runner: releasing lock after child stopped")
			signal.Reset()
			return
		}
	}
}
