package main

import (
	"fmt"
	"gopkg.in/d4l3k/messagediff.v1"
)

func main() {

	a, b := exCHI1()
	diff, equal := messagediff.PrettyDiff(a, b)
	fmt.Printf("diff:%v \nequal: %v\n=======\n", diff, equal)

	detailedDiff, equal := messagediff.DeepDiff(a, b)
	fmt.Printf("diff:%v \nequal: %v\n", detailedDiff, equal)
	processor(detailedDiff)
}
