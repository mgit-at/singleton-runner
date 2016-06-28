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
)

func main() {
	var nameTemplate = flag.String("name-template", "", "template for the lockfile name (will get expanded using environment variables)")
	var updateInterval = flag.Uint("update-interval", 30, "interval in seconds between lock file update requests")
	var updateTimeout = flag.Uint("update-timeout", 5, "timeout in seconds to wait for response from etcd")
	var gracePeriod = flag.Uint("grace-period", 30, "time in seconds to wait for a normal shutdown of the child")
	var killDelay = flag.Uint("kill-delay", 5, "")

	flag.Parse()

	if *nameTemplate == "" {
		log.Fatal("'-name-template' is empty")
	}

	var cmd string
	var args []string
	if remaining := flag.Args(); len(remaining) > 0 {
		cmd = remaining[0]
		args = remaining[1:]
	}

	log.Println("nameTemplate:", *nameTemplate)
	log.Println("updateInterval:", *updateInterval)
	log.Println("updateTimeout:", *updateTimeout)
	log.Println("gracePeriod:", *gracePeriod)
	log.Println("killDelay:", *killDelay)
	log.Printf("command: %q %q", cmd, args)
}
