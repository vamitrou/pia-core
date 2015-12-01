package connman

import (
	"errors"
	"fmt"
	"github.com/senseyeio/roger"
	"os/exec"
	"strings"
	"time"
)

type rconn struct {
	client        roger.RClient
	rserve_cmd    *exec.Cmd
	port          int
	last_accessed time.Time
}

func NewRConnection() (*rconn, error) {
	port := GetFreePort()
	rc := new(rconn)
	rc.port = port
	rc.last_accessed = time.Now()
	pwdstr := GetPWD()
	if err := rc.StartServe(pwdstr); err != nil {
		fmt.Println(err)
	}
	rClient, err := rc.GetClientWithRetries(5)
	rc.client = rClient

	return rc, err
	//return rconn{port: port, last_accessed: time.Now()}
}

func (rc rconn) Client() roger.RClient {
	return rc.client
}

func (r *rconn) Close() {
	r.StopServe()
}

func (c *rconn) StartServe(path string) error {
	cmd_str := fmt.Sprintf("%s/rserve.R %d", path, c.port)
	cmd_splits := strings.Split(cmd_str, " ")
	cmd := exec.Command(cmd_splits[0], cmd_splits[1:]...)
	c.rserve_cmd = cmd
	err := c.rserve_cmd.Start()
	return err
}

func (c *rconn) StopServe() {
	fmt.Printf("stop serving on %d\n", c.port)
	c.rserve_cmd.Process.Kill()
}

func (c *rconn) GetClientWithRetries(retries int) (roger.RClient, error) {
	connected := false
	connect_attempts := 0
	var rClient roger.RClient
	for connect_attempts < retries && !connected {
		//rClient, err := roger.NewRClient("127.0.0.1", int64(c.port))
		rClient, err := c.GetClient()
		if err == nil {
			connected = true
			return rClient, nil
		} else {
			fmt.Println(fmt.Sprintf("Cannot connect to RServe:%d, retrying in 2 seconds..", c.port))
			time.Sleep(2 * time.Second)
			connect_attempts += 1
		}
	}
	return rClient, errors.New("exceeded R Connection retries")
}

func (c *rconn) GetClient() (roger.RClient, error) {
	/*if c.client != nil {
		return c.client, nil
	}*/
	rClient, err := roger.NewRClient("127.0.0.1", int64(c.port))
	//c.client = rClient
	//return c.client, err
	return rClient, err
}
