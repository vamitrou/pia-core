package main

import (
	"fmt"
	"github.com/vamitrou/pia-core/pia4r"
	"github.com/vamitrou/pia-core/piaconf"
	"github.com/vamitrou/pia-core/piautils"
	"io/ioutil"
	"net/http"
)

var appConf *piaconf.PiaAppConf = nil

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
	if arr, ok := r.URL.Query()["callback"]; ok {
		if len(arr) > 0 {
			callback_url = arr[0]
		}
	}
	fmt.Printf("content length: %d\n", r.ContentLength)

	var data []byte
	if r.Method == "POST" {
		switch app.Language {
		case "R":
			data, err = pia4r.Process(app, body, contentType[0])
			piautils.Check(err)
		default:
			http.Error(w, fmt.Sprintf("Language %s not supported.", app.Language),
				http.StatusNotAcceptable)
			return
		}

		fmt.Printf("Callback url: %s\n", callback_url)
		if len(callback_url) > 0 {
			err = piautils.Post(callback_url, data, contentType[0])
			piautils.Check_with_abort(err, false)
		}
	} else {
		http.Error(w, fmt.Sprintf("Method not supported.", contentType[0]),
			http.StatusNotAcceptable)
	}
}

func dummy_callback(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	piautils.Check(err)
	fmt.Println(string(body))
}

func main() {
	appConf = piaconf.GetConfig()

	fmt.Println("Server started")
	http.HandleFunc("/prediction", predict)
	http.HandleFunc("/dummy_callback", dummy_callback)
	http.ListenAndServe("0.0.0.0:8000", nil)
}
