package main

import (
	"bytes"
	"fmt"
	"github.com/linkedin/goavro"
	"github.com/vamitrou/pia-core/connman"
	"io/ioutil"
	"strings"
	"time"
)

func ForwardAvroBatch(app *CatalogValue, body []byte) {
	outerStr := fmt.Sprintf("applications/%s/%s", app.Id, app.AvroIn[0])
	innerStr := fmt.Sprintf("applications/%s/%s", app.Id, app.AvroIn[1])
	_, _, codec := LoadAvroSchema(outerStr, innerStr)

	message, err := codec.Decode(bytes.NewReader(body))
	check(err)
	//fmt.Println(message)
	ProcessR(app, message)
}

func ProcessR(app *CatalogValue, data interface{}) {
	if val, ok := data.(*goavro.Record); ok {
		ConvertAvroToRDataFrame(app, val, "out.Rda")
	} else {
		// throw an error here
		return
	}

	rClient, err := connman.GetRConnection(app.Id)
	check(err)
	pwdstr := connman.GetPWD()
	_, err = rClient.Eval(fmt.Sprintf("df <- load_data('%s/applications/%s/out.Rda')", pwdstr, app.Id))
	check(err)
	fmt.Println("done")
}

func ConvertAvroToRDataFrame(app *CatalogValue, avro *goavro.Record, fname string) {
	defer timeTrack(time.Now(), "convertAvroToRdataFrame")
	var buffer bytes.Buffer
	buffer.WriteString("structure(list(\n\n")

	claims, err := avro.Get("claims")
	check(err)
	var cl []interface{}
	if clarr, ok := claims.([]interface{}); ok {
		cl = clarr
	}

	props := GetAvroFields(avro, "claims")
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
		check(err)
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
