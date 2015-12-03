package pia4r

import (
	"bytes"
	"fmt"
	"github.com/linkedin/goavro"
	"github.com/vamitrou/pia-core/connman"
	"github.com/vamitrou/pia-core/piaconf"
	"github.com/vamitrou/pia-core/piautils"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

func ForwardAvroBatch(app *piaconf.CatalogValue, body []byte, callback_url string) {
	outerStr := fmt.Sprintf("applications/%s/%s", app.Id, app.AvroIn[0])
	innerStr := fmt.Sprintf("applications/%s/%s", app.Id, app.AvroIn[1])
	_, _, codec := piautils.LoadAvroSchema(outerStr, innerStr)

	message, err := codec.Decode(bytes.NewReader(body))
	piautils.Check(err)
	//fmt.Println(message)
	data := ProcessRBatch(app, message)
	if len(callback_url) > 0 && data != nil {
		// post request here
		fmt.Printf("POST: %s\n", callback_url)
		Callback(callback_url, data)
	}
}

func ForwardJSONBatch(app *piaconf.CatalogValue, body []byte) {

}

func ProcessRBatch(app *piaconf.CatalogValue, data interface{}) []byte {
	filename := fmt.Sprintf("tmp_%d_%s", time.Now().Unix(), piautils.RandSeq(10))
	pwdstr := connman.GetPWD()
	full_file_path := fmt.Sprintf("%s/applications/%s/%s", pwdstr, app.Id, filename)
	defer os.Remove(full_file_path)
	if val, ok := data.(*goavro.Record); ok {
		// piautils.Check for errors
		ConvertAvroToRDataFrame(app, val, filename)
	} else {
		// throw an error here
		return nil
	}

	//shared := true
	live := true

	//rc, err := connman.GetRConnection(app.Id, shared)
	rc, err := connman.GetRConnection(app.Id, live) //connman.NewRConnection()
	piautils.Check(err)
	if !live {
		defer rc.Close()
	} else {
		// defer connman.Recycle(rc)
	}
	if rc == nil {
		// handle this error
		return nil
	}
	var rClient = rc.Client()
	rSession, err := rClient.GetSession()
	piautils.Check(err)
	//_, err = rClient.Eval(fmt.Sprintf("df <- load_data('%s')", full_file_path))
	rSession.SendCommand(fmt.Sprintf("df <- load_data('%s')", full_file_path)).GetResultObject()
	piautils.Check(err)
	// out, err := rClient.Eval(fmt.Sprintf("print(df)", full_file_path))
	out, err := rSession.SendCommand("print(df)").GetResultObject()
	piautils.Check(err)
	fmt.Println(out)
	fmt.Println("done")
	//connman.LazyCloseRConnection(app.Id)
	return make([]byte, 0)
}

func Callback(url string, data []byte) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if piautils.Check_with_abort(err, false) {
		return
	}
	req.Header.Set("Content-Type", "avro/binary")

	client := &http.Client{}
	resp, err := client.Do(req)
	if piautils.Check_with_abort(err, false) {
		return
	}

	defer resp.Body.Close()
	fmt.Println("response status:", resp.Status)
	fmt.Println("response headers:", resp.Header)
	body, err := ioutil.ReadAll(resp.Body)
	if piautils.Check_with_abort(err, false) {
		return
	}
	fmt.Println("response body:", string(body))

}

func ConvertAvroToRDataFrame(app *piaconf.CatalogValue, avro *goavro.Record, fname string) {
	defer piautils.TimeTrack(time.Now(), "convertAvroToRdataFrame")
	var buffer bytes.Buffer
	buffer.WriteString("structure(list(\n\n")

	claims, err := avro.Get("claims")
	piautils.Check(err)
	var cl []interface{}
	if clarr, ok := claims.([]interface{}); ok {
		cl = clarr
	}

	props := piautils.GetAvroFields(avro, "claims")
	//props := GetAvroFields(fmt.Sprintf("applications/%s/%s", app.Id, app.AvroIn[1]))
	var propStrings []string
	for i, prop := range props {
		propStrings = append(propStrings, fmt.Sprintf("\"%s\"", prop))
		buffer.WriteString(fmt.Sprintf("o%03d = ", i))

		buffer.WriteString("c(")
		if ContainsStrings(prop, cl) {
			var attrs []string
			for _, claim := range cl {

				val := strings.Replace(ToString(Get(claim, prop)), "\"", "'", -1)
				attrs = append(attrs, fmt.Sprintf("\"%s\"", val))
			}
			buffer.WriteString(strings.Join(attrs, ", "))
		} else {
			var attrs []string
			for _, claim := range cl {
				attrs = append(attrs, ToString(Get(claim, prop)))
			}
			buffer.WriteString(strings.Join(attrs, ", "))
		}
		buffer.WriteString(")")

		if i != len(props)-1 {
			buffer.WriteString(",\n")
		} else {
			buffer.WriteString("),\n")
		}
	}
	buffer.WriteString("\n.Names = c(")
	buffer.WriteString(strings.Join(propStrings, ", "))
	buffer.WriteString(fmt.Sprintf("), row.names = c(NA, -%dL), class = \"data.frame\")", len(cl)))

	//fmt.Println(buffer.String())
	ioutil.WriteFile(fmt.Sprintf("applications/%s/%s", app.Id, fname), buffer.Bytes(), 0644)
}

func ContainsStrings(prop string, claims []interface{}) bool {
	for _, claim := range claims {
		val := Get(claim, prop)
		if len(ToString(val)) == 0 {
			return true
		}
		if _, ok := val.(float64); !ok {
			return true
		}
	}
	return false
}

func Get(avro interface{}, field string) interface{} {
	if rec, ok := avro.(*goavro.Record); ok {
		val, err := rec.Get(field)
		piautils.Check(err)
		return val
	}
	return nil
}

func ToString(value interface{}) string {
	if val, ok := value.(string); ok {
		return val
	} else if val, ok := value.(float64); ok {
		return fmt.Sprintf("%f", val)
	}
	return ""
}
