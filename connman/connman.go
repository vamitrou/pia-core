package connman

import (
	"errors"
	"fmt"
	"github.com/senseyeio/roger"
	"time"
)

var connections map[string]interface{} = make(map[string]interface{})

func GetRConnection(app_id string) (roger.RClient, error) {
	// get connection
	if con, ok := connections[app_id]; ok {
		rcon, ok := con.(*rconn)
		if !ok {
			return nil, errors.New("couldn't not assert connection type")
		}
		return rcon.client, nil
	}

	// no connection, let's create one
	// get port
	port := GetFreePort()

	rc := NewRConnection(port)
	connections[app_id] = rc
	pwdstr := GetPWD()
	if err := rc.StartServe(pwdstr); err != nil {
		fmt.Println(err)
	}

	return rc.GetClientWithRetries(3)
}

func CloseRConnection(app_id string) {
	if c, ok := connections[app_id]; ok {
		if val, ok := c.(*rconn); ok {
			val.last_accessed = time.Now()
		}
	}
	go PurgeRConnection(app_id)
}

func PurgeRConnection(app_id string) error {
	keep_alive_seconds := float64(20)
	if c, ok := connections[app_id]; ok {
		if val, ok := c.(*rconn); ok {
			dt := time.Since(val.last_accessed).Seconds()
			if dt >= keep_alive_seconds {
				val.StopServe()
				delete(connections, app_id)
			} else {
				d, _ := time.ParseDuration(fmt.Sprintf("%fs", keep_alive_seconds-dt))
				time.Sleep(d)
				defer PurgeRConnection(app_id)
			}
		}
	} else {
		return errors.New(fmt.Sprintf("No active connection for app id: %s", app_id))
	}
	return nil
}
