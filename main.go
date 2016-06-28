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
	"flag"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
	"time"
)

const (
	LOCK_FILE_BASE = "/singleton.mgit.at/"
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

func acquireLock(lockfile string, ttl time.Duration) (err error) {
	log.Printf("trying to acquire lock: %s (TTL: %v)", lockfile, ttl)
	// TODO: try to create lock file, if it exists attach to it and wait for updated/deleted event
	// when updated exit
	// when deleted try again
	return
}

func updateLock(lockfile string, ttl, timeout time.Duration) (err error) {
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

	if err := acquireLock(lockfilePath, ttl); err != nil {
		log.Fatal("singleton-runner: unable to acquier lock:", err)
	}
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
			if err := updateLock(lockfilePath, ttl, time.Duration(*updateTimeout)*time.Second); err != nil {
				log.Println("singleton-runner: updateting lock failed:", err)
				// send TERM signal to client
				// if after gracePeriod child is still running send a KILL signal
			}
		case <-exited:
			log.Println("singleton-runner: closing...")
			return
		}
	}
}
