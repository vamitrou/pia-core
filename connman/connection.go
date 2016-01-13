package connman

import (
	"errors"
	"fmt"
	"github.com/senseyeio/roger"
	"github.com/vamitrou/pia-core/piaconf"
	"github.com/vamitrou/pia-core/pialog"
	"github.com/vamitrou/pia-core/piautils"
	"os/exec"
	"strings"
	"time"
)

type rconn struct {
	client        roger.RClient
	session       roger.Session
	rserve_cmd    *exec.Cmd
	port          int
	last_accessed time.Time
}

func NewRConnection() (*rconn, error) {
	port := GetFreePort()
	rc := new(rconn)
	rc.port = port
	rc.last_accessed = time.Now()
	pwdstr := piautils.GetPWD()
	if err := rc.StartServe(pwdstr); err != nil {
		fmt.Println(err)
	}
	rClient, err := rc.GetClientWithRetries(5)
	rc.client = rClient

	return rc, err
}

func (rc rconn) Client() roger.RClient {
	return rc.client
}

func (rc *rconn) Session(reqId string, app *piaconf.CatalogValue) (roger.Session, error) {
	var err error
	if rc.session == nil {
		rc.session, err = rc.client.GetSession()
		if err != nil {
			rc.session = nil
		}
		pwd := piautils.GetPWD()
		rc.session.SendCommand(fmt.Sprintf("setwd('%s/applications/%s/')", pwd, app.Id))
		cmd := fmt.Sprintf("source ('%s')", app.InitScript)
		// check for errors
		pialog.Trace(reqId, "Loading init script")
		start := time.Now()
		rc.session.SendCommand(cmd)
		pialog.Trace(reqId, "Init script loaded in", time.Since(start))
	}
	return rc.session, err
}

func (rc *rconn) CloseSession() {
	rc.session.Close()
	rc.session = nil
}

func (r *rconn) Close(reqId string) {
	r.StopServe()
	pialog.Trace(reqId, "Stopped serving R on", r.port)
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
	c.rserve_cmd.Process.Kill()
}

func (c *rconn) GetClientWithRetries(retries int) (roger.RClient, error) {
	connected := false
	connect_attempts := 0
	var rClient roger.RClient

	start_time := time.Now()
	for connect_attempts < retries && !connected {
		//rClient, err := roger.NewRClient("127.0.0.1", int64(c.port))
		rClient, err := c.GetClient()
		if err == nil {
			connected = true
			return rClient, nil
		} else {
			if time.Since(start_time) > 10*time.Second {
				fmt.Println(fmt.Sprintf("Cannot connect to RServe:%d, aborting after 10 seconds..", c.port))
				break
			}
			time.Sleep(100 * time.Millisecond)
			//connect_attempts += 1
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
