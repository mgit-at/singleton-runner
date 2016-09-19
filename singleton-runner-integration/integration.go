package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

func installEtcd(version string) (string, error) {
	var (
		url      = fmt.Sprintf("https://github.com/coreos/etcd/releases/download/v%[1]s/etcd-v%[1]s-linux-amd64.tar.gz", version)
		filename = fmt.Sprintf("etcd-v%s-linux-amd64.tar.gz", version)
		dirname  = fmt.Sprintf("etcd-v%s-linux-amd64", version)
	)

	if version == "" {
		return "", errors.New("missing etcd version")
	}

	// download
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		log.Println("downloading", filename)
		curlPath, err := exec.LookPath("curl")
		if err != nil {
			return "", err
		}
		cmd := exec.Command(curlPath, "-L", url, "-o", filename)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}

	// extract
	if _, err := os.Stat(dirname); os.IsNotExist(err) {
		log.Println("extracting", dirname)
		tarPath, err := exec.LookPath("tar")
		if err != nil {
			return "", err
		}
		cmd := exec.Command(tarPath, "xzvf", filename)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return "", err
		}
	}

	path, err := filepath.Abs(dirname)
	if err != nil {
		return "", err
	}

	return path, nil
}

type etcdNode struct {
	id         int
	name       string
	peerAddr   string
	clientAddr string
	dataDir    string
	logName    string
	logFile    io.WriteCloser

	cmd *exec.Cmd
}

func newNode(id int) *etcdNode {
	return &etcdNode{
		id:         id,
		name:       fmt.Sprintf("etcd%d", id),
		peerAddr:   fmt.Sprintf("http://127.0.0.1:%d", 2380+2*(id-1)),
		clientAddr: fmt.Sprintf("http://127.0.0.1:%d", 2379+2*(id-1)),
		dataDir:    fmt.Sprintf("etcd%d.etcd", id),
		logName:    fmt.Sprintf("etcd%d.log", id),
	}
}

func (n *etcdNode) start(etcdPath, initialCluster, initialState string, deleteData bool) error {
	if deleteData {
		if err := os.RemoveAll(n.dataDir); err != nil {
			return err
		}
	}

	logFile, err := os.Create(n.logName)
	if err != nil {
		return err
	}
	n.logFile = logFile

	log.Printf("starting node %s (peerAddr=%s, clientAddr=%s)\n", n.name, n.peerAddr, n.clientAddr)

	n.cmd = exec.Command(
		filepath.Join(etcdPath, "etcd"),
		"--name", n.name,
		"--data-dir", n.dataDir,
		"--initial-advertise-peer-urls", n.peerAddr,
		"--listen-peer-urls", n.peerAddr,
		"--listen-client-urls", n.clientAddr,
		"--advertise-client-urls", n.clientAddr,
		"--initial-cluster-token", "etcd-cluster",
		"--initial-cluster", initialCluster,
		"--initial-cluster-state", initialState,
	)
	n.cmd.Stderr = logFile
	n.cmd.Stdout = logFile

	if err := n.cmd.Start(); err != nil {
		return err
	}

	return nil
}

func (n *etcdNode) stop() error {
	if n.cmd != nil {
		if err := n.cmd.Process.Signal(os.Interrupt); err != nil {
			return err
		}
		if err := n.cmd.Wait(); err != nil {
			if _, ok := err.(*exec.ExitError); !ok {
				return err
			}
		}
		n.cmd = nil
	}

	if n.logFile != nil {
		if err := n.logFile.Close(); err != nil {
			return err
		}
		n.logFile = nil
	}

	return nil
}

func waitAddr(addr string) error {
	var conn net.Conn
	var err error
	ticker := time.NewTicker(5 * time.Second)
	for i := 0; i < 5; i++ {
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		<-ticker.C
	}
	ticker.Stop()
	return err
}

func waitNodes(nodes []*etcdNode) error {
	for _, node := range nodes {
		addr := strings.TrimPrefix(node.clientAddr, "http://")
		if err := waitAddr(addr); err != nil {
			return err
		}
	}
	return nil
}

func getClientAddrs(nodes []*etcdNode) string {
	buf := &bytes.Buffer{}
	for i := range nodes {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(nodes[i].clientAddr)
	}
	return buf.String()
}

func getInitialCluster(nodes []*etcdNode) string {
	buf := &bytes.Buffer{}
	for i := range nodes {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(buf, "%s=%s", nodes[i].name, nodes[i].peerAddr)
	}
	return buf.String()
}

func startCluster(etcdPath string, ids ...int) (nodes []*etcdNode, err error) {
	nodes = make([]*etcdNode, len(ids))
	for i, id := range ids {
		nodes[i] = newNode(id)
	}
	defer func() {
		if err != nil {
			for _, node := range nodes {
				node.stop()
			}
		}
	}()

	initialCluster := getInitialCluster(nodes)
	log.Println("creating cluster", initialCluster)
	for _, node := range nodes {
		if err := node.start(etcdPath, initialCluster, "new", false); err != nil {
			return nil, err
		}
	}

	if err := waitNodes(nodes); err != nil {
		return nil, err
	}
	log.Println("cluster ready")

	return nodes, nil
}

func main() {
	var (
		etcdVersion = flag.String("etcd-version", "", "etcd version number")
		flagTest    = flag.String("test", ".*", "regular expression to match test names")
	)
	flag.Parse()

	if flag.NArg() > 0 {
		flag.Usage()
		os.Exit(1)
	}

	reTest, err := regexp.Compile(*flagTest)
	if err != nil {
		log.Fatalln("invalid regexp:", err)
	}

	etcdPath, err := installEtcd(*etcdVersion)
	if err != nil {
		log.Fatalln("failed to install etcd:", err)
	}

	tests := []struct {
		Name string
		Func func(string) error
	}{
		{"TestSimple", TestSimple},
		{"TestLooseQuorum", TestLooseQuorum},
	}
	first := true
	for _, test := range tests {
		if !reTest.MatchString(test.Name) {
			continue
		}

		if !first {
			fmt.Println()
		}
		first = false

		fmt.Println("=====", test.Name)

		if err := test.Func(etcdPath); err != nil {
			log.Println("Test failed:", err)
		} else {
			log.Println("Success")
		}
	}
}

func TestSimple(etcdPath string) error {
	nodes, err := startCluster(etcdPath, 1, 2, 3)
	if err != nil {
		log.Fatalln("failed to start cluster")
	}
	defer func() {
		for _, node := range nodes {
			if err := node.stop(); err != nil {
				log.Println("failed to stop node:", err)
			}
		}
	}()

	singletonPath, err := exec.LookPath("singleton-runner")
	if err != nil {
		return err
	}
	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		return err
	}

	cmd := exec.Command(
		singletonPath,
		"--name-template", "simple",
		"--instance-template", "instance-1",
		"--etcd", getClientAddrs(nodes),
		sleepPath, "5",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}

func TestLooseQuorum(etcdPath string) error {
	nodes, err := startCluster(etcdPath, 1, 2)
	if err != nil {
		log.Fatalln("failed to start cluster")
	}
	defer func() {
		for _, node := range nodes {
			if err := node.stop(); err != nil {
				log.Println("failed to stop node:", err)
			}
		}
	}()

	singletonPath, err := exec.LookPath("singleton-runner")
	if err != nil {
		return err
	}
	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		return err
	}

	var (
		requestTimeout = 30
		gracePeriod    = 1
		killBackoff    = 1
		etcdStartup    = 5
	)
	ttl := int((3*(requestTimeout+gracePeriod+killBackoff))/2) + 1

	cmd := exec.Command(
		singletonPath,
		"--name-template", "noquorum",
		"--instance-template", "instance-1",
		"--etcd", getClientAddrs(nodes),
		"--request-timeout", strconv.Itoa(requestTimeout),
		"--grace-period", strconv.Itoa(gracePeriod),
		"--kill-backoff", strconv.Itoa(killBackoff),
		sleepPath, strconv.Itoa(2*ttl),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	startTime := time.Now()
	if err := cmd.Start(); err != nil {
		return err
	}

	cancel := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		select {
		case <-time.After(2 * time.Second):
			log.Println("stopping", nodes[0].name)
			if err := nodes[0].stop(); err != nil {
				log.Printf("failed to stop %s: %v\n", nodes[0].name, err)
			} else {
				log.Println(nodes[0].name, "stopped")
			}
		case <-cancel:
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		select {
		case <-time.After(time.Duration(ttl-killBackoff-gracePeriod-etcdStartup) * time.Second):
			log.Println("starting", nodes[0].name)
			if err := nodes[0].start(etcdPath, getInitialCluster(nodes), "existing", false); err != nil {
				log.Printf("failed to start %s: %v\n", nodes[0].name, err)
			} else {
				if err := waitNodes(nodes[:1]); err != nil {
					log.Printf("failed to wait for %s: %v\n", nodes[0].name, err)
				} else {
					log.Println(nodes[0].name, "ready")
				}
			}
		case <-cancel:
		}
		wg.Done()
	}()

	defer func() {
		close(cancel)
		wg.Wait()
	}()

	if err := cmd.Wait(); err != nil {
		return err
	}

	if time.Now().Sub(startTime) < time.Duration(ttl-killBackoff-gracePeriod)*time.Second {
		return errors.New("singleton-runner exited too early")
	}

	return nil
}
