package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/altinity/clickhouse-operator/dev/RnD/chopsim/parser"
	"gopkg.in/yaml.v2"
)

type dataYAML struct{}

func (d dataYAML) Marshal(buffer *bytes.Buffer, object interface{}) {
	b, err := yaml.Marshal(object)
	if err != nil {
		log.Fatal("Unable to marshal manifest data -> ", err)
	}
	buffer.Write(b)
}

func main() {
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatal("Unable to read data from the standard input -> ", err)
	}
	chi := &parser.ClickHouseInstallation{}
	if err := yaml.Unmarshal(data, chi); err != nil {
		log.Fatal("Unable to unmarshal manifest data -> ", err)
	}
	d := dataYAML{}
	fmt.Println(parser.GenerateArtifacts(chi, d))
}
