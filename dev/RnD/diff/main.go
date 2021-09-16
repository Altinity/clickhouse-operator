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

package main

import (
	"fmt"
	"github.com/r3labs/diff"
)

type order struct {
	ID    string `diff:"id"`
	Items []int  `diff:"items"`
}

func main() {

	a, b := ex4()

	fmt.Printf("test diff\n")
	changelog, err := diff.Diff(a, b)

	if err != nil {
		fmt.Printf("Error %v\n", err)
	}

	fmt.Printf("cl: %v", changelog)
}

func ex4() ([]int, []int) {
	a := []int{1, 1}
	b := []int{1}

	return a, b
}

func ex3() ([]int, []int) {
	a := []int{1, 2, 3, 4}
	b := []int{1, 2, 3}

	return a, b
}
