package main

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

func ex1() (struct1, struct1) {
	a := struct1{
		1,
		2,
	}
	b := struct1{
		1,
		3,
	}

	return a, b
}

func ex2() (struct2, struct2) {
	a := struct2{
		A: 1,
		b: 2,
		C: []int{1},
	}
	b := struct2{
		A: 1,
		b: 3,
		C: []int{1, 1, 2},
	}

	return a, b
}

func ex3() ([]int, []int) {
	a := []int{1, 2}
	b := []int{1}

	return a, b
}

func ex4() ([]int, []int) {
	a := []int{1, 1}
	b := []int{1}

	return a, b
}

func ex5() ([]int, []int) {
	a := []int{1}
	b := []int{1, 1}

	return a, b
}

func ex6() (struct4, struct4) {
	a := struct4{
		s3: struct3{
			s2: struct2{
				A: 1,
				b: 2,
				C: []int{1},
			},
		},
	}

	b := struct4{
		s3: struct3{
			s2: struct2{
				A: 1,
				b: 3,
				C: []int{1, 1, 2},
			},
		},
	}

	return a, b
}

func ex7() (struct5, struct5) {
	a := struct5{
		s4: []struct4{
			{
				s3: struct3{
					s2: struct2{
						A: 1,
						b: 2,
						C: []int{1},
					},
				},
			},
		},
	}

	b := struct5{
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
						A: 1,
						b: 2,
						C: []int{1},
					},
				},
			},
		},
	}

	return a, b
}

func ex8() (struct5, struct5) {
	a := struct5{
		s4: []struct4{
			{
				s3: struct3{
					s2: struct2{
						A: 1,
						b: 2,
						C: []int{1},
					},
				},
			},
		},
	}

	b := struct5{
		s4: []struct4{
			{
				s3: struct3{
					s2: struct2{
						A: 1,
						b: 2,
						C: []int{1},
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

	return a, b
}

func ex9() (struct5, struct5) {
	a := struct5{
		s4: []struct4{
			{
				s3: struct3{
					s2: struct2{
						A: 1,
						b: 2,
						C: []int{1},
					},
				},
			},
		},
	}

	b := struct5{
		s4: []struct4{
			{
				s3: struct3{
					s2: struct2{
						A: 1,
						b: 2,
						C: []int{1, 1},
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

	return a, b
}
