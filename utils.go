package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/linkedin/goavro"
	"io/ioutil"
	"time"
)

func check(e error) {
	check_with_abort(e, true)
}

func check_with_abort(e error, abort bool) {
	if e != nil {
		if abort {
			panic(e)
		} else {
			fmt.Println(e)
		}
	}
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", name, elapsed)
}

func LoadAvroSchema(outerFile string, innerFile string) (goavro.RecordSetter, goavro.RecordSetter, goavro.Codec) {
	dat, err := ioutil.ReadFile(innerFile)
	check(err)
	innerSchemaStr := string(dat)

	dat2, err := ioutil.ReadFile(outerFile)
	check(err)
	outerSchemaStr := fmt.Sprintf(string(dat2), innerSchemaStr)

	outerSchema := goavro.RecordSchema(outerSchemaStr)
	innerSchema := goavro.RecordSchema(innerSchemaStr)
	codec, err := goavro.NewCodec(outerSchemaStr)
	check(err)
	return outerSchema, innerSchema, codec
}

func _GetAvroFields(filename string) []string {
	dat, err := ioutil.ReadFile(filename)
	check(err)
	var j map[string]interface{}
	err = json.Unmarshal(dat, &j)
	check(err)
	fields_arr, ok := j["fields"].([]interface{})
	if !ok {
		check(errors.New("invalid fields json schema"))
	}
	props := make([]string, 0)
	for _, field := range fields_arr {
		field_map, ok := field.(map[string]interface{})
		if !ok {
			check(errors.New("invalid field json schema"))
		}
		propname, _ := field_map["name"].(string)
		props = append(props, propname)
	}
	return props
}

func GetAvroFields(record *goavro.Record, object string) []string {
	schema, _ := record.GetFieldSchema(object)
	items := schema.(map[string]interface{})["items"]
	fields := items.(map[string]interface{})["fields"].([]interface{})
	ret_fields := make([]string, 0)
	for _, field := range fields {
		f := field.(map[string]interface{})["name"].(string)
		ret_fields = append(ret_fields, f)
	}
	return ret_fields
}
