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

	fmt.Printf("content length: %d\n", r.ContentLength)

	if r.Method == "GET" {
		if contentType[0] == "application/json" {
			pia4r.ForwardJSONBatch(app, body)
		} else {
			http.Error(w, fmt.Sprintf("Content-Type %s not supported.", contentType[0]),
				http.StatusNotAcceptable)
		}
	} else if r.Method == "POST" {
		if contentType[0] == "avro/binary" {
			if app == nil {
				http.Error(w, "", http.StatusBadGateway)
			}

			callback_url := ""
			if arr, ok := r.URL.Query()["callback"]; ok {
				if len(arr) > 0 {
					callback_url = arr[0]
				}
			} else {
				http.Error(w, "Callback url is required for POST requests.",
					http.StatusNotAcceptable)
			}
			if app.Language == "R" {
				pia4r.ForwardAvroBatch(app, body, callback_url)
			} else {
				http.Error(w, fmt.Sprintf("Language %s not supported.", app.Language),
					http.StatusNotAcceptable)
			}

		} else {
			http.Error(w, fmt.Sprintf("Content-Type %s not supported.", contentType[0]),
				http.StatusNotAcceptable)
		}

	}
}

func main() {
	appConf = piaconf.GetConfig()

	fmt.Println("Server started")
	http.HandleFunc("/prediction", predict)
	http.ListenAndServe("0.0.0.0:8000", nil)
}

/*func test_avro() {

	record_schema := `{
	    "name": "Claim",
	    "type": "record",
	    "fields": [
	    {
		"name": "GD_OE_ID", "type": "int"
	    },
	    {
		"name": "TEST_FIELD", "type": "string"
	    }
	    ]
	}`

	schema := `{
		"name": "Claims",
		"type": "record",
		"fields": [
		    {
			"name": "claims",
			"type": "array",
			"items": %s
		    }
		]
	    }`

	codec, _ := goavro.NewCodec(fmt.Sprintf(schema, record_schema))
	fmt.Println(codec)

	var claims []interface{}
	s := goavro.RecordSchema(record_schema)
	fmt.Println(s)
	claim, _ := goavro.NewRecord(goavro.RecordSchema(record_schema))
	claim.Set("GD_OE_ID", int64(1))
	claim.Set("TEST_FIELD", "tessssst")
	claims = append(claims, claim)

	rSchema := goavro.RecordSchema(fmt.Sprintf(schema, record_schema))
	outerRecord, err := goavro.NewRecord(rSchema)
	if err != nil {
		fmt.Println("ERROR:")
		fmt.Println(err)
	}
	outerRecord.Set("claims", claims)

	GetAvroFields(outerRecord, "claims")
}

func GetAvroFields(record *goavro.Record, object string) []string {
	schema, _ := record.GetFieldSchema(object)
	items := schema.(map[string]interface{})["items"]
	fields := items.(map[string]interface{})["fields"].([]interface{})
	ret_fields := make([]string, len(fields))
	for _, field := range fields {
		f := field.(map[string]interface{})["name"].(string)
		fmt.Println(f)
		ret_fields = append(ret_fields, f)
	}
	return ret_fields
}*/
