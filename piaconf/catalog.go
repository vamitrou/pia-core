package piaconf

import (
	"errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

var catalog_path string = ""
var appConf *PiaAppConf = nil

type PiaAppConf struct {
	Applications []CatalogValue `yaml:"applications"`
}

type CatalogValue struct {
	Id         string   `yaml:"id"`
	Name       string   `yaml:"name"`
	Language   string   `yaml:"prog_lang"`
	Command    string   `yaml:"entry_cmd"`
	AvroIn     []string `yaml:"avro_in"`
	AvroOut    []string `yaml:"avro_out"`
	InitScript string   `yaml:"init_script"`
	ExecCmd    string   `yaml:"exec_cmd"`
}

func (c *PiaAppConf) Load(path string) error {
	catalog_path = path
	dat, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(dat, &c)
	return err
}

func LoadConfig(path string) error {
	catalog_path = path
	_, err := GetConfig()
	return err
}

func GetConfig() (*PiaAppConf, error) {
	var err error
	if appConf == nil {
		if len(catalog_path) == 0 {
			return nil, errors.New("Catalog not loaded.")
		}
		appConf = new(PiaAppConf)
		err = appConf.Load(catalog_path)
	}
	return appConf, err
}

func GetApp(appId string) (*CatalogValue, error) {
	conf, err := GetConfig()
	if err != nil {
		return &CatalogValue{}, err
	}
	for _, app := range conf.Applications {
		if app.Id == appId {
			return &app, nil
		}
	}
	return &CatalogValue{}, errors.New("App not found")
}
