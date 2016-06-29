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
	"errors"
	"flag"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

const (
	LOCK_FILE_BASE = "/singleton.mgit.at"
)

func runChild(cmd string, args []string, signals <-chan os.Signal) (err error) {
	child := exec.Cmd{}
	child.Path = cmd
	child.Args = args
	child.Stdin = os.Stdin
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr

	if err = child.Start(); err != nil {
		return err
	}

	go func() {
		for sig := range signals {
			child.Process.Signal(sig)
		}
	}()
	return child.Wait()
}

func initETCdClient(updateTimeout time.Duration) (client.KeysAPI, error) {
	cfg := client.Config{
		Endpoints:               []string{"http://127.0.0.1:2379"}, // TODO: make this configurable
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: updateTimeout,
	}
	c, err := client.New(cfg)
	if err != nil {
		return nil, err
	}
	kapi := client.NewKeysAPI(c)
	return kapi, err
}

func IsKeyExists(err error) bool {
	if cErr, ok := err.(client.Error); ok {
		return cErr.Code == client.ErrorCodeNodeExist
	}
	return false
}

func acquireLock(kapi client.KeysAPI, lockfile string, ttl time.Duration) (err error) {
	log.Printf("singleton-runner: trying to acquire lock: %s (TTL: %v)", lockfile, ttl)

	opts := &client.SetOptions{PrevExist: client.PrevNoExist, TTL: ttl, Refresh: false}
	if _, err = kapi.Set(context.Background(), lockfile, "kubernetes-pod-id", opts); err == nil { // TODO: use kubernetes pod name as value...
		return
	}
	if !IsKeyExists(err) {
		return
	}
	log.Printf("singleton-runner: lock is already acquired - watching for changes")
	err = errors.New("watching lockfile not yet implemented!")
	// TODO: attach to lockfile and wait for updated/deleted event
	// when updated exit
	// when deleted try again
	return
}

func releaseLock(kapi client.KeysAPI, lockfile string) {
	log.Printf("trying to release lock: %s", lockfile)
}

func updateLock(kapi client.KeysAPI, lockfile string, ttl, timeout time.Duration) (err error) {
	log.Printf("trying to update lock: %s (TTL: %v, Timeout: %v)", lockfile, ttl, timeout)
	// Try to update the lock: return err if this fails
	return
}

func main() {
	var nameTemplate = flag.String("name-template", "", "template for the lockfile name (will get expanded using environment variables)")
	var updateInterval = flag.Uint("update-interval", 30, "interval in seconds between lock file update requests")
	var updateTimeout = flag.Uint("update-timeout", 5, "timeout in seconds to wait for response from etcd")
	var gracePeriod = flag.Uint("grace-period", 30, "time in seconds to wait for a normal shutdown of the child")
	var killDelay = flag.Uint("kill-delay", 5, "")

	flag.Parse()

	if *nameTemplate == "" {
		log.Fatal("singleton-runner: '-name-template' is empty")
	}
	name := os.ExpandEnv(*nameTemplate)
	lockfilePath := filepath.Join(LOCK_FILE_BASE, path.Clean("/"+name))

	var cmd string
	args := flag.Args()
	if args := flag.Args(); len(args) > 0 {
		cmd = args[0]
	} else {
		log.Fatal("singleton-runner: please specify a command to run")
	}

	ttl := time.Duration(*updateInterval+*updateTimeout+*gracePeriod+*killDelay) * time.Second

	kapi, err := initETCdClient(time.Duration(*updateTimeout) * time.Second)
	if err != nil {
		log.Fatal("error connecting to etcd:", err)
	}
	// defer releaseLock(kapi, lockfilePath) // should this be done in any case?

	if err := acquireLock(kapi, lockfilePath, ttl); err != nil {
		log.Fatal("singleton-runner: unable to acquire lock: ", err)
	}

	log.Printf("singleton-runner: lock acquired successfully! .. starting %q", cmd)
	exited := make(chan bool, 1)

	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM)
	go func() {
		defer func() {
			exited <- true
		}()
		err := runChild(cmd, args, signals)
		if err != nil {
			log.Printf("singleton-runner: child exited with: %v", err)
		} else {
			log.Printf("singleton-runner: child exited normally")
		}
	}()

	t := time.NewTicker(time.Duration(*updateInterval) * time.Second)
	for {
		select {
		case <-t.C:
			if err := updateLock(kapi, lockfilePath, ttl, time.Duration(*updateTimeout)*time.Second); err != nil {
				log.Println("singleton-runner: updateting lock failed:", err)
				// send TERM signal to client
				// if after gracePeriod child is still running send a KILL signal
			}
		case <-exited:
			log.Println("singleton-runner: closing...")
			releaseLock(kapi, lockfilePath) // remove here if we do this using the defer above
			return
		}
	}
}
