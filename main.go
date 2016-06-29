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
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

const (
	LOCK_FILE_BASE = "/singleton.mgit.at"
)

var (
// Machines contains a list of all etcd machines (http address)
)

// command line flags
var (
	flagNameTemplate   = flag.String("name-template", "", "template for the lockfile name (will get expanded using environment variables)")
	flagInstTemplate   = flag.String("instance-template", "", "template for the instance name (will get expanded using environment variables)")
	flagRequestTimeout = flag.Uint("request-timeout", 5, "timeout in seconds to wait for responses from etcd")
	flagUpdateInterval = flag.Uint("update-interval", 30, "interval in seconds between lock file update requests")
	flagGracePeriod    = flag.Uint("grace-period", 30, "time in seconds to wait for a normal shutdown of the child")
	flagKillBackoff    = flag.Uint("kill-delay", 5, "")
	flagEtcd           = flag.String("etcd", "http://127.0.0.1:2379", "etcd machines (comma separated list)")
	flagCA             = flag.String("ca", "", "CA certificate")
	flagCert           = flag.String("cert", "", "client certificate")
	flagKey            = flag.String("key", "", "client key")
)

func runChild(cmd string, args []string, signals <-chan os.Signal) (exited chan bool, err error) {
	child := exec.Cmd{}
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

	go func() {
		for sig := range signals {
			child.Process.Signal(sig)
		}
	}()
	return
}

func initETCdClient(machines []string, CA, cert, key string, timeout time.Duration) (etcd.KeysAPI, error) {
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
		Endpoints: machines,
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: tlsConfig,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		HeaderTimeoutPerRequest: 5 * time.Second,
	}

	c, err := etcd.New(cfg)
	if err != nil {
		return nil, err
	}
	kapi := etcd.NewKeysAPI(c)
	return kapi, err
}

func IsKeyExists(err error) bool {
	if cErr, ok := err.(etcd.Error); ok {
		return cErr.Code == etcd.ErrorCodeNodeExist
	}
	return false
}

func acquireLock(kapi etcd.KeysAPI, lockfile, instanceID string, ttl, timeout time.Duration) (err error) {
	for {
		log.Printf("singleton-runner: trying to acquire lock: %s (TTL: %v)", lockfile, ttl)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		opts := &etcd.SetOptions{PrevExist: etcd.PrevNoExist, TTL: ttl, Refresh: false}
		_, err = kapi.Set(ctx, lockfile, instanceID, opts)
		cancel()
		if err == nil {
			return // we got the lock!
		}
		if !IsKeyExists(err) {
			return // this seems to be a severe problem.. better give up
		}
		log.Printf("singleton-runner: lock is already acquired - watching for changes")

		ctx, cancel = context.WithTimeout(context.Background(), ttl)
		watcher := kapi.Watcher(lockfile, nil)
	WatchLock:
		for {
			resp, err := watcher.Next(ctx)
			if err != nil {
				log.Printf("singleton-runner: watching for events failed: %v", err)
				return err
			}
			switch resp.Action {
			case "expire":
				fallthrough
			case "compareAndDelete":
				fallthrough
			case "delete":
				log.Printf("singleton-runner: delete or expire event - try again!")
				break WatchLock
			case "compareAndSwap":
				return errors.New("locked instance seems to be alive - giving up")
			default:
				log.Printf("singleton-runner: ignoring unknown event '%s'", resp.Action)
			}
		}
	}
	return
}

func releaseLock(kapi etcd.KeysAPI, lockfile, instanceID string) {
	log.Printf("singleton-runner: trying to release lock: %s", lockfile)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	opts := &etcd.DeleteOptions{PrevValue: instanceID}
	_, err := kapi.Delete(ctx, lockfile, opts)
	cancel()
	if err != nil {
		log.Printf("singleton-runner: releasing lock failed: %s", err)
	}
	return
}

func updateLock(kapi etcd.KeysAPI, lockfile, instanceID string, ttl, timeout time.Duration) (err error) {
	log.Printf("singleton-runner: trying to update lock: %s (TTL: %v, Timeout: %v)", lockfile, ttl, timeout)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	opts := &etcd.SetOptions{PrevValue: instanceID, PrevExist: etcd.PrevExist, TTL: ttl, Refresh: false}
	_, err = kapi.Set(ctx, lockfile, instanceID, opts)
	cancel()
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
	lockfilePath := filepath.Join(LOCK_FILE_BASE, path.Clean("/"+name))

	if *flagInstTemplate == "" {
		log.Fatal("singleton-runner: '-instance-template' is empty")
	}
	instanceID := os.ExpandEnv(*flagInstTemplate)

	requestTimeout := time.Duration(*flagRequestTimeout) * time.Second
	updateInterval := time.Duration(*flagUpdateInterval) * time.Second
	gracePeriod := time.Duration(*flagGracePeriod) * time.Second
	killBackoff := time.Duration(*flagKillBackoff) * time.Second
	ttl := updateInterval + requestTimeout + gracePeriod + killBackoff

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

	kapi, err := initETCdClient(machines, CA, cert, key, requestTimeout)
	if err != nil {
		log.Fatal("error connecting to etcd:", err)
	}
	// defer releaseLock(kapi, lockfilePath, instanceID) // should this be done in any case?

	//
	// **** try to get the lock
	//
	if err := acquireLock(kapi, lockfilePath, instanceID, ttl, requestTimeout); err != nil {
		log.Fatal("singleton-runner: unable to acquire lock: ", err)
	}
	log.Printf("singleton-runner: lock acquired successfully! .. starting %q", cmd)

	//
	// **** run the child
	//
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	exited, err := runChild(cmd, args, signals)
	if err != nil {
		releaseLock(kapi, lockfilePath, instanceID) // remove here if we do this using the defer above
		log.Fatal("singleton-runner: calling child failed:", err)
	}

	//
	// **** update the lock - release lock if child exits
	//
	t := time.NewTicker(updateInterval)
Loop:
	for {
		select {
		case <-t.C:
			if err := updateLock(kapi, lockfilePath, instanceID, ttl, requestTimeout); err != nil {
				log.Println("singleton-runner: updateting lock failed:", err)
				log.Println("singleton-runner: sending child the TERM signal")
				signals <- syscall.SIGTERM
				break Loop
			}
		case <-exited:
			log.Println("singleton-runner: closing...")
			releaseLock(kapi, lockfilePath, instanceID) // remove here if we do this using the defer above
			return
		}
	}

	//
	// **** wait for child to exit - kill it after grace period
	//
	k := time.NewTimer(gracePeriod)
	select {
	case <-k.C:
		log.Println("singleton-runner: grace period elapsed sending child the KILL signal")
		signals <- syscall.SIGKILL
	case <-exited:
		log.Println("singleton-runner: exiting after child stopped")
		releaseLock(kapi, lockfilePath, instanceID) // remove here if we do this using the defer above
		return
	}
}
