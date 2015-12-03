package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

var appConf *PiaAppConf = nil

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

	if r.Method == "GET" {
		// http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		if contentType[0] == "application/json" {
			body, err := ioutil.ReadAll(r.Body)
			check(err)
			ForwardJSONBatch(app, body)
		} else {
			http.Error(w, fmt.Sprintf("Content-Type %s not supported.", contentType[0]),
				http.StatusNotAcceptable)
			return
		}
	} else if r.Method == "POST" {
		if contentType[0] == "avro/binary" {
			body, err := ioutil.ReadAll(r.Body)
			check(err)

			fmt.Printf("content length: %d\n", r.ContentLength)

			app := new(CatalogValue)
			err = GetApp(application[0], app)
			check(err)
			if app == nil {
				http.Error(w, "", http.StatusBadGateway)
			}

			callback_url := ""
			if arr, ok := r.URL.Query()["callback"]; ok {
				if len(arr) > 0 {
					callback_url = arr[0]
				}
			}
			ForwardAvroBatch(app, body, callback_url)

		} else {
			http.Error(w, fmt.Sprintf("Content-Type %s not supported.", contentType[0]),
				http.StatusNotAcceptable)
			return
		}

	}
}

func main() {
	appConf = GetConfig()

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
