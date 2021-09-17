package main

import "fmt"

func main() {
	m1 := make(map[string]string)
	fmt.Println("m11=", m1)
	fmt.Println("-----------")

	m1["k1"] = "v1"
	fmt.Println("m12=", m1)
	fmt.Println("-----------")

	m2 := m1
	fmt.Println("m13=", m1)
	fmt.Println("m23=", m2)
	fmt.Println("-----------")

	m2["k2"] = "v1"
	m2["k3"] = "v3"
	fmt.Println("m14=", m1)
	fmt.Println("m24=", m2)
	fmt.Println("-----------")

	delete(m1, "k1")
	fmt.Println("m15=", m1)
	fmt.Println("m25=", m2)
	fmt.Println("-----------")

	delete(m2, "k2")
	fmt.Println("m16=", m1)
	fmt.Println("m26=", m2)
	fmt.Println("-----------")

	m1 = nil
	fmt.Println("m17=", m1)
	fmt.Println("m27=", m2)
	fmt.Println("-----------")

	m1 = make(map[string]string)
	fmt.Println("m18=", m1)
	fmt.Println("m28=", m2)
	fmt.Println("-----------")

	m1["k1"] = "v1"
	fmt.Println("m19=", m1)
	fmt.Println("m29=", m2)
	fmt.Println("-----------")

	// func makemap(t *maptype, hint int, h *hmap) *hmap
	// type hmap struct {
	//	// Note: the format of the hmap is also encoded in cmd/compile/internal/gc/reflect.go.
	//	// Make sure this stays in sync with the compiler's definition.
	//	count     int // # live cells == size of map.  Must be first (used by len() builtin)
	//	flags     uint8
	//	B         uint8  // log_2 of # of buckets (can hold up to loadFactor * 2^B items)
	//	noverflow uint16 // approximate number of overflow buckets; see incrnoverflow for details
	//	hash0     uint32 // hash seed
	//
	//	buckets    unsafe.Pointer // array of 2^B Buckets. may be nil if count==0.
	//	oldbuckets unsafe.Pointer // previous bucket array of half the size, non-nil only when growing
	//	nevacuate  uintptr        // progress counter for evacuation (buckets less than this have been evacuated)
	//
	//	extra *mapextra // optional fields
	//}
}
