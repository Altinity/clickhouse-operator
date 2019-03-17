package main

import (
	"fmt"
	"gopkg.in/d4l3k/messagediff.v1"
)

type someStruct1 struct {
	A int
	// To ignore a field in a struct, just annotate it with testdiff:"ignore" like this:
	B int `testdiff:"ignore"`
}

type someStruct2 struct {
	A, b int
	C []int
}

func main() {
	a := someStruct1{
		1,
		2,
	}
	b := someStruct1{
		1,
		3,
	}
	diff, equal := messagediff.PrettyDiff(a, b)
	fmt.Printf("diff:%v \n equal: %v", diff, equal)

	a1 := someStruct2{
		A:1,
		b:2,
		C:[]int{1},
	}
	b1 := someStruct2{
		A:1,
		b:3,
		C:[]int{1, 1, 2},
	}
	diff, equal = messagediff.PrettyDiff(a1, b1)
	fmt.Printf("diff:%v \n equal: %v", diff, equal)

	a2 := []int{1,2}
	b2 := []int{1}
	diff, equal = messagediff.PrettyDiff(a2, b2)
	fmt.Printf("diff:%v \n equal: %v", diff, equal)

	a3 := []int{1,1}
	b3 := []int{1}
	diff, equal = messagediff.PrettyDiff(a3, b3)
	fmt.Printf("diff:%v \n equal: %v", diff, equal)

	a4 := []int{1}
	b4 := []int{1,1}
	diff, equal = messagediff.PrettyDiff(a4, b4)
	fmt.Printf("diff:%v \n equal: %v", diff, equal)

	// See the DeepDiff function for using the diff results programmatically.
	detailedDiff, equal := messagediff.DeepDiff(a4, b4)
	fmt.Printf("\ndiff:%v \n equal: %v", detailedDiff, equal)

	detailedDiff, equal = messagediff.DeepDiff(a1, b1)
	fmt.Printf("\ndiff:%v \n equal: %v", detailedDiff, equal)
}
