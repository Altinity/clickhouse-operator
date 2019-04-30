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
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

type xmlNode struct {
	children []*xmlNode
	tag      string
	value    interface{}
}

const (
	eol   = "\n"
	noEol = ""
)

// GenerateXML creates XML representation from the provided input
func GenerateXML(w io.Writer, input map[string]interface{}, indent, tabsize uint8, excludes ...string) {
	re := regexp.MustCompile("//+")

	// paths is sorted set of normalized paths (maps keys) from 'input'
	paths := make([]string, 0, len(input))

	// data is copy of 'input' with:
	// 1. paths (map keys) are normalized in terms of trimmed '/'
	// 2. all excludes are excluded
	data := make(map[string]interface{})
	// Skip excluded paths
	for key, value := range input {
		// key may be non-normalized, and may have starting or trailing '/'
		// path is normalized path without starting and trailing '/', ex.: 'test/quotas'
		path := re.ReplaceAllString(strings.Trim(key, "/"), "/")
		if path == "" || checkExcludes(path, excludes) {
			continue
		}
		paths = append(paths, path)
		data[path] = value
	}
	sort.Strings(paths)

	// xmlTreeRoot - root of the XML tree data structure
	xmlTreeRoot := new(xmlNode)

	// Read all tags and values into the tree structure
	for _, path := range paths {
		// Split path (test/quotas) into tags which would be 'test' and 'quota'
		tags := strings.Split(path, "/")
		if len(tags) == 0 {
			// Empty path? Should not be, but double check
			continue
		}
		xmlTreeRoot.addBranch(tags, data[path])
	}

	// return XML
	xmlTreeRoot.buildXML(w, indent, tabsize)
}

// addBranch ensures branch esists and assign value to the last tagged node
func (n *xmlNode) addBranch(tags []string, value interface{}) {
	node := n
	for _, tag := range tags {
		node = node.addChild(tag)
	}
	node.value = value
}

// addChild add new or return existing child with matching tag
func (n *xmlNode) addChild(tag string) *xmlNode {
	if n.children == nil {
		n.children = make([]*xmlNode, 0, 0)
	}

	// Check for such tag exists
	for i := range n.children {
		if n.children[i].tag == tag {
			// Already have such a tag
			return n.children[i]
		}
	}

	// No this is new tag - add as a child
	node := &xmlNode{
		tag: tag,
	}
	n.children = append(n.children, node)

	return node
}

// buildXML generates XML from xmlNode type linked list
func (n *xmlNode) buildXML(w io.Writer, indent, tabsize uint8) {
	switch n.value.(type) {
	case []interface{}:
		// Value is an array of strings
		// Repeat tag with each value, like:
		// <yandex>
		//    ...
		//    <networks>
		//        <ip>::/64</ip>
		//        <ip>203.0.113.0/24</ip>
		//        <ip>2001:DB8::/32</ip>
		for _, value := range n.value.([]interface{}) {
			stringValue := value.(string)
			n.printTagWithValue(w, stringValue, indent, tabsize)
		}
	case string:
		// value is a string
		stringValue := n.value.(string)
		n.printTagWithValue(w, stringValue, indent, tabsize)
	default:
		// no value node, may have nested tags
		n.printTagNoValue(w, indent, tabsize)
	}
}

// printTagNoValue prints tag which has no value, But it may have nested tags
// <a>
//  <b>...</b>
// </a>
func (n *xmlNode) printTagNoValue(w io.Writer, indent, tabsize uint8) {
	n.printTag(w, indent, true, eol)
	for i := range n.children {
		n.children[i].buildXML(w, indent+tabsize, tabsize)
	}
	n.printTag(w, indent, false, eol)
}

// printTagWithValue prints tag with value. But it must have no children,
// and children are not printed
// <tag>value</tag>
func (n *xmlNode) printTagWithValue(w io.Writer, value string, indent, tabsize uint8) {
	n.printTag(w, indent, true, noEol)
	n.printValue(w, value)
	n.printTag(w, 0, false, eol)
}

// printTag prints XML tag into io.Writer
func (n *xmlNode) printTag(w io.Writer, indent uint8, openTag bool, eol string) {
	if n.tag == "" {
		return
	}

	// We have to separate indent and no-indent cases, because event target pattern is like
	// "%0s</%s> - meaning we do not want to print leading spaces, having " " in Fprint inserts one space
	if indent > 0 {
		pattern := ""
		if openTag {
			// pattern would be: %4s<%s>%s
			pattern = fmt.Sprintf("%%%ds<%%s>%%s", indent)
		} else {
			// pattern would be: %4s</%s>%s
			pattern = fmt.Sprintf("%%%ds</%%s>%%s", indent)
		}
		_, _ = fmt.Fprintf(w, pattern, " ", n.tag, eol)
	} else {
		if openTag {
			// pattern would be: %4s<%s>%s
			_, _ = fmt.Fprintf(w, "<%s>%s", n.tag, eol)
		} else {
			// pattern would be: %4s</%s>%s
			_, _ = fmt.Fprintf(w, "</%s>%s", n.tag, eol)
		}
	}
}

// printTag prints XML value into io.Writer
func (n *xmlNode) printValue(w io.Writer, value string) {
	_, _ = fmt.Fprintf(w, "%s", value)
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
