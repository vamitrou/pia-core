package pia4r

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/linkedin/goavro"
	"github.com/vamitrou/pia-core/connman"
	"github.com/vamitrou/pia-core/piaconf"
	"github.com/vamitrou/pia-core/pialog"
	"github.com/vamitrou/pia-core/piautils"
	"strings"
	"time"
)

func Process(reqId string, app *piaconf.CatalogValue, body []byte, contentType string, live bool) ([]byte, error) {
	contentType = strings.Trim(strings.Split(contentType, ";")[0], " ")
	switch contentType {
	case "avro/binary":
		return processAvro(reqId, app, body, live)
	case "application/json":
		return processJSON(reqId, app, body, live)
	default:
		return nil, errors.New(fmt.Sprintf("Not supported Content Type: %s", contentType))
	}
}

func processAvro(reqId string, app *piaconf.CatalogValue, body []byte, live bool) ([]byte, error) {
	outerStr := fmt.Sprintf("%s/applications/%s/%s", piautils.AppDir(), app.Id, app.AvroIn[0])
	innerStr := fmt.Sprintf("%s/applications/%s/%s", piautils.AppDir(), app.Id, app.AvroIn[1])
	_, _, codec := piautils.LoadAvroSchema(outerStr, innerStr)

	message, err := codec.Decode(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	// filename := fmt.Sprintf("tmp_%d_%s", time.Now().Unix(), piautils.RandSeq(10))
	filename := reqId
	if avroRec, ok := message.(*goavro.Record); ok {
		dur, err := convertToRDataFrame(app, avroRec, filename)
		if err != nil {
			return nil, err
		}
		pialog.Trace(reqId, "Convert to data frame took:", dur)
		defer DeleteTempFile(app, filename)
	} else {
		return nil, errors.New("Could not convert body to Avro.")
	}

	pwdstr := piautils.GetPWD()
	filepath := fmt.Sprintf("%s/applications/%s/tmp/%s", pwdstr, app.Id, filename)
	data, err := processDataFrame(reqId, app, filepath, live)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func processDataFrame(reqId string, app *piaconf.CatalogValue, filepath string, live bool) ([]byte, error) {
	//live := true

	rc, err := connman.GetRConnection(reqId, app.Id, live) //connman.NewRConnection()
	if err != nil {
		return nil, err
	}
	if !live {
		defer rc.Close(reqId)
	} else {
		// defer connman.Recycle(rc)
	}
	if rc == nil {
		return nil, errors.New("Could not get connection.")
	}
	rSession, err := rc.Session(reqId, app)
	if err != nil {
		return nil, err
	}
	if !live {
		defer rSession.Close()
	}
	start := time.Now()
	rSession.SendCommand(fmt.Sprintf("df <- dget('%s')", filepath))
	pialog.Trace(reqId, "Dataframe loaded in", time.Since(start))
	//fmt.Println("loading init script")
	//pwdstr := piautils.GetPWD()
	//source_cmd := fmt.Sprintf("source('%s/applications/%s/%s')", pwdstr, app.Id, app.InitScript)
	//rSession.SendCommand(source_cmd)

	//piautils.Check(err)
	//fmt.Println("init script loaded successfully")
	cmd := strings.Replace(app.ExecCmd, "$in", "df", -1)
	start = time.Now()
	out, err := rSession.SendCommand(cmd).GetResultObject()
	if err != nil {
		error_filepath := strings.Replace(filepath, "/tmp/", "/error_data/", 1)
		pialog.Warn(reqId, "Failed execution, input will be copied to", error_filepath)
		pialog.Error(out)
		piautils.EnsureDir(app, "error_data")
		piautils.CopyFile(filepath, error_filepath)
		return nil, err
	}
	pialog.Trace(reqId, "Command executed in:", time.Since(start))
	//fmt.Println(out)

	if str_val, ok := out.(string); ok {
		return []byte(str_val), nil
	} else {
		return nil, errors.New("R output is not string. (WHAAAAAA)")
	}
}

func processJSON(reqId string, app *piaconf.CatalogValue, body []byte, live bool) ([]byte, error) {
	var j []map[string]interface{}
	err := json.Unmarshal(body, &j)
	if err != nil {
		return nil, errors.New("Malformed JSON input")
	}
	if live && len(j) > 10 {
		return nil, errors.New("Synchronous API allows maximum 10 elements")
	} else if len(j) > 3000 {
		return nil, errors.New("Unsupported batch size: %d, maximum size: 3000")
	}
	filename := fmt.Sprintf("tmp_%d_%s", time.Now().Unix(), piautils.RandSeq(10))
	dur, err := convertToRDataFrame(app, j, filename)
	if err != nil {
		return nil, err
	}
	pialog.Trace(reqId, "Convert to data frame took:", dur)
	defer DeleteTempFile(app, filename)
	//fmt.Println(j)
	pwdstr := piautils.GetPWD()
	filepath := fmt.Sprintf("%s/applications/%s/tmp/%s", pwdstr, app.Id, filename)
	//fmt.Println(filepath)
	data, err := processDataFrame(reqId, app, filepath, live)
	if err != nil {
		return nil, err
	}
	return data, nil
	//return nil, nil
}
