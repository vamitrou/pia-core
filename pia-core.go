package main

import (
	"fmt"
	"io"
	//"github.com/vamitrou/pia-core/connman"
	"errors"
	"github.com/vamitrou/pia-core/pia4r"
	"github.com/vamitrou/pia-core/piaconf"
	"github.com/vamitrou/pia-core/piautils"
	"io/ioutil"
	"net/http"
)

var appConf *piaconf.PiaAppConf = nil

func ServePost(app *piaconf.CatalogValue, contentType string, body []byte) ([]byte, error) {
	switch app.Language {
	case "R":
		return pia4r.Process(app, body, contentType, synchronous)
	default:
		return nil, errors.New(fmt.Sprintf("Language %s not supported.", app.Language))
	}
}

func predict(w http.ResponseWriter, r *http.Request) {
	contentType := r.Header["Content-Type"]
	application := r.Header["Application"]
	if len(contentType) == 0 {
		http.Error(w, "Missing Content-Type", http.StatusNotAcceptable)
		return
	}
	if len(application) == 0 {
		http.Error(w, "Missing application header", http.StatusNotAcceptable)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	piautils.Check(err)

	app := new(piaconf.CatalogValue)
	err = piaconf.GetApp(application[0], app)
	piautils.Check(err)

	callback_url := ""
	synchronous := true
	if arr, ok := r.URL.Query()["callback"]; ok {
		if len(arr) > 0 {
			callback_url = arr[0]
			synchronous = false
		}
	}
	fmt.Printf("content length: %d\n", r.ContentLength)

	if r.Method == "POST" {
		if synchronous {
			w.Header().Set("Content-Type", contentType[0])
			data, err := ServePost(app, contentType[0], body, "")
			if err != nil {
				io.WriteString(w, err.Error())
			} else {
				io.WriteString(w, string(data))
			}
		} else {
			go func() {
				data, err := ServePost(app, contentType[0], body, callback_url)
				if err != nil {
					err := piautils.Post(callback_url, []byte(err.Error()), "application/json")
					piautils.Check_with_abort(err, false)
				} else {
					err := piautils.Post(callback_url, data, contentType[0])
					piautils.Check_with_abort(err, false)
				}
			}()
			io.WriteString(w, fmt.Sprintf("Response will be posted to: %s", callback_url))
		}

	} else {
		http.Error(w, fmt.Sprintf("Method not supported.", contentType[0]),
			http.StatusNotAcceptable)
	}
}

func main() {
	appConf = piaconf.GetConfig()
	//connman.WarmUpConnections(appConf)

	fmt.Println("Server started")
	http.HandleFunc("/prediction", predict)
	http.ListenAndServe("0.0.0.0:8000", nil)
}
