package pia4r

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/linkedin/goavro"
	"github.com/vamitrou/pia-core/connman"
	"github.com/vamitrou/pia-core/piaconf"
	"github.com/vamitrou/pia-core/piautils"
	"time"
)

func Process(app *piaconf.CatalogValue, body []byte, contentType string) ([]byte, error) {
	switch contentType {
	case "avro/binary":
		return processAvro(app, body)
	case "application/json":
		return processJSON(app, body)
	default:
		return nil, errors.New(fmt.Sprintf("Not supported Content Type: %s", contentType))
	}
}

func processAvro(app *piaconf.CatalogValue, body []byte) ([]byte, error) {
	outerStr := fmt.Sprintf("applications/%s/%s", app.Id, app.AvroIn[0])
	innerStr := fmt.Sprintf("applications/%s/%s", app.Id, app.AvroIn[1])
	_, _, codec := piautils.LoadAvroSchema(outerStr, innerStr)

	message, err := codec.Decode(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	filename := fmt.Sprintf("tmp_%d_%s", time.Now().Unix(), piautils.RandSeq(10))
	if avroRec, ok := message.(*goavro.Record); ok {
		err = convertAvroToRDataFrame(app, avroRec, filename)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("Could not convert body to Avro.")
	}

	pwdstr := piautils.GetPWD()
	filepath := fmt.Sprintf("%s/applications/%s/%s", pwdstr, app.Id, filename)
	data, err := processDataFrame(app, filepath)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func processDataFrame(app *piaconf.CatalogValue, filepath string) ([]byte, error) {
	live := true

	rc, err := connman.GetRConnection(app.Id, live) //connman.NewRConnection()
	piautils.Check(err)
	if !live {
		defer rc.Close()
	} else {
		// defer connman.Recycle(rc)
	}
	if rc == nil {
		return nil, errors.New("Could not get connection.")
	}
	rSession, err := rc.Session()
	if err != nil {
		return nil, err
	}
	if !live {
		defer rSession.Close()
	}
	piautils.Check(err)
	rSession.SendCommand(fmt.Sprintf("df <- load_data('%s')", filepath)).GetResultObject()
	piautils.Check(err)
	rSession.SendCommand("library(rjson)")
	rSession.SendCommand("x <- toJSON(unname(split(df, 1:nrow(df))))")
	out, err := rSession.SendCommand("print(x)").GetResultObject()
	piautils.Check(err)
	fmt.Println(out)
	fmt.Println("done")

	if bytes_val, ok := out.(string); ok {
		return []byte(bytes_val), nil
	} else {
		return nil, errors.New("R output is not string. (WHAAAAAA)")
	}
}

func processJSON(app *piaconf.CatalogValue, body []byte) ([]byte, error) {
	var j map[string]interface{}
	err := json.Unmarshal(body, &j)
	piautils.Check(err)
	fmt.Println(j)
	return nil, nil
}
