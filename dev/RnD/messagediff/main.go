package main

import (
	"fmt"
	"gopkg.in/d4l3k/messagediff.v1"
)

type struct1 struct {
	A int
	// To ignore a field in a struct, just annotate it with testdiff:"ignore" like this:
	B int `testdiff:"ignore"`
}

type struct2 struct {
	A int
	b int
	C []int
}

type struct3 struct {
	s2 struct2
}

type struct4 struct {
	s3 struct3
}

type struct5 struct {
	s4 []struct4
}

func main() {
	a := struct1{
		1,
		2,
	}
	b := struct1{
		1,
		3,
	}
	diff, equal := messagediff.PrettyDiff(a, b)
	fmt.Printf("diff:%v \n equal: %v", diff, equal)

	a1 := struct2{
		A:1,
		b:2,
		C:[]int{1},
	}
	b1 := struct2{
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
	fmt.Printf("\ndiff:%v \n equal: %v", *detailedDiff, equal)

	a5 := struct4 {
		s3: struct3{
			s2: struct2{
				A:1,
				b:2,
				C:[]int{1},
			},
		},
	}

	b5 := struct4 {
		s3: struct3{
			s2: struct2{
				A:1,
				b:3,
				C:[]int{1, 1, 2},
			},
		},
	}

	detailedDiff, equal = messagediff.DeepDiff(a5, b5)
	fmt.Printf("\ndiff:%v \n equal: %v", *detailedDiff, equal)

	a6 := struct5{
		s4: []struct4{
			{
				s3: struct3{
					s2: struct2{
						A:1,
						b:2,
						C:[]int{1},
					},
				},
			},
		},
	}

	b6 := struct5{
		s4: []struct4{
			{
				s3: struct3{
					s2: struct2{
						A: 1,
						b: 3,
						C: []int{1, 1, 2},
					},
				},
			},
			{
				s3: struct3{
					s2: struct2{
						A:1,
						b:2,
						C:[]int{1},
					},
				},
			},
		},
	}

	detailedDiff, equal = messagediff.DeepDiff(a6, b6)
	fmt.Printf("\ndiff:%v \n equal: %v", *detailedDiff, equal)

	a7 := struct5{
		s4: []struct4{
			{
				s3: struct3{
					s2: struct2{
						A:1,
						b:2,
						C:[]int{1},
					},
				},
			},
		},
	}

	b7 := struct5{
		s4: []struct4{
			{
				s3: struct3{
					s2: struct2{
						A:1,
						b:2,
						C:[]int{1},
					},
				},
			},
			{
				s3: struct3{
					s2: struct2{
						A: 1,
						b: 3,
						C: []int{1, 1, 2},
					},
				},
			},
		},
	}

	detailedDiff, equal = messagediff.DeepDiff(a7, b7)
	fmt.Printf("\ndiff:%v \n equal: %v", *detailedDiff, equal)
}
