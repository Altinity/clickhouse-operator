// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xml

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

type xmlNode struct {
	childs []*xmlNode
	tag    string
	value  string
}

const xmlBuildError = "Unable to build XML: incorrect structure of definitions"

// GenerateXML creates XML representation from the provided input
func GenerateXML(w io.Writer, input map[string]string, depth, step uint8, excludes ...string) error {
	// preparing the data
	re := regexp.MustCompile("//+")
	keys := make([]string, 0, len(input))
	data := make(map[string]string)
	for k, v := range input {
		nk := re.ReplaceAllString(strings.Trim(k, "/"), "/")
		if nk == "" || checkExcludes(nk, excludes) {
			continue
		}
		keys = append(keys, nk)
		data[nk] = v
	}
	sort.Strings(keys)
	// xmlTree - root of the XML tree data structure
	xmlTree := &xmlNode{
		childs: make([]*xmlNode, 0, len(data)),
	}
	// reading all tags and values into the tree structure
	for _, k := range keys {
		v := data[k]
		var val string
		tags := strings.Split(k, "/")
		last := len(tags) - 1
		if last < 0 {
			continue
		} else if last == 0 {
			val = v
		}
		node := xmlTree.addChild(&xmlNode{
			tag:   tags[0],
			value: val,
		})
		for j := 1; j <= last; j++ {
			if j == last {
				val = v
			}
			node = node.addChild(&xmlNode{
				tag:   tags[j],
				value: val,
			})
		}
	}
	// building XML document
	return xmlTree.buildXML(w, depth, step)
}

// addChild checks list of xmlNode childs in order to match corresponding tags,
// if no match - it sets the node as a new child, otherwise returns matched child
func (n *xmlNode) addChild(node *xmlNode) *xmlNode {
	if len(n.childs) > 0 {
		for i := range n.childs {
			// set existing child as a reference to the node with the same tag
			if n.childs[i].tag == node.tag {
				return n.childs[i]
			}
		}
	}
	// setting the "node" parameter as a new child of the node "n"
	n.childs = append(n.childs, node)
	return node
}

// buildXML generates XML from xmlNode type linked list
func (n *xmlNode) buildXML(w io.Writer, depth, step uint8) error {
	hasChilds := len(n.childs) > 0
	x := ""
	if hasChilds {
		if n.value != "" {
			return errors.New(xmlBuildError)
		}
		x = "\n"
	}
	n.printTag(w, depth, " ", "", x)
	for i := range n.childs {
		err := n.childs[i].buildXML(w, depth+step, step)
		if err != nil {
			return err
		}
	}
	space := " "
	d := depth
	if !hasChilds {
		fmt.Fprintf(w, "%s", n.value)
		space = ""
		d = 0
	}
	n.printTag(w, d, space, "/", "\n")
	return nil
}

// printTag prints XML tag with io.Writer
func (n *xmlNode) printTag(w io.Writer, d uint8, sp, s, l string) {
	if n.tag != "" {
		pattern := fmt.Sprintf("%%%ds<%%s%%s>%%s", d)
		fmt.Fprintf(w, pattern, sp, s, n.tag, l)
	}
}

// checkExcludes returns true if first tag of the key matches item with excludes list
func checkExcludes(key string, excludes []string) bool {
	tags := strings.Split(key, "/")
	if len(tags) == 0 {
		return false
	}
	for j := range excludes {
		if tags[0] == excludes[j] {
			return true
		}
	}
	return false
}
