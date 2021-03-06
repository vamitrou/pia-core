package pia4r

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/linkedin/goavro"
	"github.com/vamitrou/pia-core/piaconf"
	"github.com/vamitrou/pia-core/piautils"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"time"
)

func convertToRDataFrame(app *piaconf.CatalogValue, data interface{}, fname string) (time.Duration, error) {
	//defer piautils.TimeTrack(time.Now(), "convertToRDataFrame")
	start := time.Now()
	var buffer bytes.Buffer

	EnsureTempDir(app)

	buffer.WriteString("structure(list(\n\n")

	var claims_arr []interface{}
	var props []string
	if avro, ok := data.(*goavro.Record); ok {
		avro_claims, err := avro.Get("claims")
		if err != nil {
			return -1, err
		}
		claims_arr = avro_claims.([]interface{})
		props = piautils.GetAvroFields(avro, "claims")
	} else if j_arr, ok := data.([]map[string]interface{}); ok {
		//claims_arr = make([]interface{}, len(j_arr))
		for _, claim := range j_arr {
			claims_arr = append(claims_arr, claim)
		}
		//claims_arr = j_arr
		if len(claims_arr) == 0 {
			return -1, errors.New("List of JSON input is empty")
		}
		el := claims_arr[0].(map[string]interface{})
		props = piautils.GetJSONFields(el)
	} else {
		return -1, errors.New(fmt.Sprintf("unsupported input: %s", reflect.TypeOf(data)))
	}

	var propStrings []string
	for i, prop := range props {
		propStrings = append(propStrings, fmt.Sprintf("\"%s\"", prop))
		buffer.WriteString(fmt.Sprintf("o%03d = ", i))

		buffer.WriteString("c(")
		if ContainsStrings(prop, claims_arr) {
			var attrs []string
			for _, claim := range claims_arr {

				//val := strings.Replace(ToString(Get(claim, prop)), "\"", "'", -1)
				val := EscapeValue(ToString(Get(claim, prop)))
				attrs = append(attrs, fmt.Sprintf("\"%s\"", val))
			}
			buffer.WriteString(strings.Join(attrs, ", "))
		} else {
			var attrs []string
			for _, claim := range claims_arr {
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
	buffer.WriteString(fmt.Sprintf("), row.names = c(NA, -%dL), class = \"data.frame\")", len(claims_arr)))

	//fmt.Println(buffer.String())
	return time.Since(start), ioutil.WriteFile(fmt.Sprintf("%s/applications/%s/tmp/%s", piautils.AppDir(), app.Id, fname), buffer.Bytes(), 0644)
}

func EscapeValue(val string) string {
	val = strings.Replace(val, "\"", "'", -1)
	val = strings.Replace(val, "\\", " ", -1)
	return val
}

func DeleteTempFile(app *piaconf.CatalogValue, filename string) {
	pwd := piautils.GetPWD()
	os.Remove(fmt.Sprintf("%s/applications/%s/tmp/%s", pwd, app.Id, filename))
}

func EnsureTempDir(app *piaconf.CatalogValue) {
	piautils.EnsureDir(app, "tmp")
}

func ContainsStrings(prop string, claims []interface{}) bool {
	for _, claim := range claims {
		val := Get(claim, prop)
		if _, ok := val.(string); ok {
			return true
		}
	}
	return false
}

func Get(data interface{}, field string) interface{} {
	if rec, ok := data.(*goavro.Record); ok {
		val, err := rec.Get(field)
		piautils.Check(err)
		return val
	} else if rec, ok := data.(map[string]interface{}); ok {
		val := rec[field]
		return val
	} else {
		fmt.Println("is not avro or json record")
	}
	return nil
}

func ToString(value interface{}) string {
	if val, ok := value.(string); ok {
		return val
	} else if val, ok := value.(float64); ok {
		return fmt.Sprintf("%f", val)
	} else if val, ok := value.(int64); ok {
		return fmt.Sprintf("%d", val)
	}
	return ""
}
