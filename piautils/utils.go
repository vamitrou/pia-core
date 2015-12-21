package piautils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/linkedin/goavro"
	"github.com/vamitrou/pia-core/piaconf"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

func Check(e error) {
	Check_with_abort(e, true)
}

func Check_with_abort(e error, abort bool) bool {
	if e != nil {
		if abort {
			panic(e)
		} else {
			fmt.Println(e)
			return true
		}
	}
	return false
}

func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", name, elapsed)
}

func GetPWD() string {
	pwd, _ := exec.Command("pwd").Output()
	pwdstr := strings.Trim(string(pwd), "\n\t\r")
	return pwdstr
}

func EnsureDir(app *piaconf.CatalogValue, dirname string) {
	pwd := GetPWD()
	app_tmp_dir := fmt.Sprintf("%s/applications/%s/%s", pwd, app.Id, dirname)
	_, err := os.Stat(app_tmp_dir)
	if err != nil {
		os.Mkdir(app_tmp_dir, 0777)
	}
}

func CopyFile(src, dst string) (err error) {
	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return
		}
	}
	if err = os.Link(src, dst); err == nil {
		return
	}
	err = copyFileContents(src, dst)
	return
}

func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

func RandSeq(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func Post(url string, data []byte, contentType string) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	/*fmt.Println("response status:", resp.Status)
	fmt.Println("response headers:", resp.Header)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Println("response body:", string(body))*/
	return nil
}

func LoadAvroSchema(outerFile string, innerFile string) (goavro.RecordSetter, goavro.RecordSetter, goavro.Codec) {
	dat, err := ioutil.ReadFile(innerFile)
	Check(err)
	innerSchemaStr := string(dat)

	dat2, err := ioutil.ReadFile(outerFile)
	Check(err)
	outerSchemaStr := fmt.Sprintf(string(dat2), innerSchemaStr)

	outerSchema := goavro.RecordSchema(outerSchemaStr)
	innerSchema := goavro.RecordSchema(innerSchemaStr)
	codec, err := goavro.NewCodec(outerSchemaStr)
	Check(err)
	return outerSchema, innerSchema, codec
}

func _GetAvroFields(filename string) []string {
	dat, err := ioutil.ReadFile(filename)
	Check(err)
	var j map[string]interface{}
	err = json.Unmarshal(dat, &j)
	Check(err)
	fields_arr, ok := j["fields"].([]interface{})
	if !ok {
		Check(errors.New("invalid fields json schema"))
	}
	props := make([]string, 0)
	for _, field := range fields_arr {
		field_map, ok := field.(map[string]interface{})
		if !ok {
			Check(errors.New("invalid field json schema"))
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

func GetJSONFields(record map[string]interface{}) []string {
	keys := make([]string, 0)
	for k, _ := range record {
		keys = append(keys, k)
	}
	return keys
}
