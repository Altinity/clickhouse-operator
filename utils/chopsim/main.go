package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/altinity/clickhouse-operator/utils/chopsim/parser"
	"gopkg.in/yaml.v2"
)

func main() {
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatal("Unable to read data from the standard input -> ", err)
	}

	chi := &parser.ClickHouseInstallation{}
	if err := yaml.Unmarshal(data, chi); err != nil {
		log.Fatal("Unable to unmarshal manifest data -> ", err)
	}

	output, err := parser.GenerateArtifacts(chi)
	if err != nil {
		log.Fatal("Unable to create resulting manifest", err)
	}
	fmt.Println(output)
}
