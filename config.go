package main

import (
	"errors"
	"github.com/vamitrou/pia-core/Godeps/_workspace/src/github.com/BurntSushi/toml"
)

var _pia *PiaCoreConf = nil

type PiaCoreConf struct {
	Local LocalConf `toml:"local"`
}

type LocalConf struct {
	Listen      string
	Port        int32
	CatalogPath string `toml:"catalog_path"`
}

func (c *PiaCoreConf) Load(path string) error {
	if _, err := toml.DecodeFile(path, c); err != nil {
		return errors.New("Could not load pia-core.toml")
	}
	return nil
}

func GetPiaConfig(path string) (*PiaCoreConf, error) {
	var err error
	if _pia == nil {
		_pia = new(PiaCoreConf)
		err = _pia.Load(path)
	}
	return _pia, err
}
