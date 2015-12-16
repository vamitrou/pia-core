package main

import (
	"errors"
	"fmt"
	"github.com/pborman/uuid"
	"github.com/vamitrou/pia-core/pia4r"
	"github.com/vamitrou/pia-core/piaconf"
	"github.com/vamitrou/pia-core/pialog"
	"github.com/vamitrou/pia-core/piautils"
	"io"
	"io/ioutil"
	"net/http"
)

var appConf *piaconf.PiaAppConf = nil

func ServePost(app *piaconf.CatalogValue, contentType string, body []byte, synchronous bool) ([]byte, error) {
	switch app.Language {
	case "R":
		return pia4r.Process(app, body, contentType, synchronous)
	default:
		return nil, errors.New(fmt.Sprintf("Language %s not supported.", app.Language))
	}
}

func predict(w http.ResponseWriter, r *http.Request) {
	rid := uuid.New()
	pialog.Info(rid, "Incoming request from:", r.Host)
	contentType := r.Header["Content-Type"]
	application := r.Header["Application"]
	if len(contentType) == 0 {
		pialog.Error(rid, "Missing Content-Type")
		http.Error(w, "Missing Content-Type", http.StatusNotAcceptable)
		return
	}
	if len(application) == 0 {
		pialog.Error(rid, "Missing application header")
		http.Error(w, "Missing application header", http.StatusNotAcceptable)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	piautils.Check(err)

	app, err := piaconf.GetApp(application[0])
	piautils.Check(err)

	callback_url := ""
	synchronous := true
	if arr, ok := r.URL.Query()["callback"]; ok {
		if len(arr) > 0 {
			callback_url = arr[0]
			synchronous = false
		}
	}

	pialog.Info(rid, r.Method, "synchronous:", synchronous, "Content-Length:",
		r.ContentLength, "Application:", application[0], "Content-Type:", contentType[0])

	if r.Method == "POST" {
		if synchronous {
			data, err := ServePost(app, contentType[0], body, synchronous)
			if err != nil {
				pialog.Error(rid, "Error serving POST request:", err.Error())
				io.WriteString(w, err.Error())
			} else {
				w.Header().Set("Content-Type", contentType[0])
				io.WriteString(w, string(data))
			}
		} else {
			go func() {
				data, err := ServePost(app, contentType[0], body, synchronous)
				piautils.Check_with_abort(err, false)
				if err != nil {
					j_err := []byte(fmt.Sprintf("{\"error\": \"%s\"}", err.Error()))
					err := piautils.Post(callback_url, j_err, "application/json")
					if err != nil {
						pialog.Error(rid, "Error serving POST request:", err.Error())
					}
				} else {
					err := piautils.Post(callback_url, data, contentType[0])
					if err != nil {
						pialog.Error(rid, "Error serving POST request:", err.Error())
					}
				}
			}()
			io.WriteString(w, fmt.Sprintf("Response will be posted to: %s", callback_url))
		}

	} else {
		pialog.Error(rid, "Method", r.Method, "not supported")
		http.Error(w, fmt.Sprintf("Method not supported.", contentType[0]),
			http.StatusNotAcceptable)
	}
}

func main() {
	appCatalogPath := "catalog.yml"
	serverAddress := "0.0.0.0"
	serverPort := 8000
	version := 0.1

	pialog.InitializeLogging()

	pialog.Info("Starting pia-core version:", version)
	pialog.Info("Loading applications config:", appCatalogPath)

	err := piaconf.LoadConfig(appCatalogPath)
	if err != nil {
		pialog.Error(err.Error())
		panic(err)
	}

	pialog.Info("Applications config loaded")

	pialog.Info("Server started: ", serverAddress, ":", serverPort)
	http.HandleFunc("/prediction", predict)
	err = http.ListenAndServe(fmt.Sprintf("%s:%d", serverAddress, serverPort), nil)
	if err != nil {
		pialog.Error("Could not start server")
		pialog.Error(err.Error())
	}
}
