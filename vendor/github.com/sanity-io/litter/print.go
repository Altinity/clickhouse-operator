package litter

import (
	"io"
	"strconv"
)

func printBool(w io.Writer, value bool) {
	if value {
		w.Write([]byte("true"))
		return
	}
	w.Write([]byte("false"))
}

func printInt(w io.Writer, val int64, base int) {
	w.Write([]byte(strconv.FormatInt(val, base)))
}

func printUint(w io.Writer, val uint64, base int) {
	w.Write([]byte(strconv.FormatUint(val, base)))
}

func printFloat(w io.Writer, val float64, precision int) {
	w.Write([]byte(strconv.FormatFloat(val, 'g', -1, precision)))
}

func printComplex(w io.Writer, c complex128, floatPrecision int) {
	w.Write([]byte("complex"))
	printInt(w, int64(floatPrecision*2), 10)
	r := real(c)
	w.Write([]byte("("))
	w.Write([]byte(strconv.FormatFloat(r, 'g', -1, floatPrecision)))
	i := imag(c)
	if i >= 0 {
		w.Write([]byte("+"))
	}
	w.Write([]byte(strconv.FormatFloat(i, 'g', -1, floatPrecision)))
	w.Write([]byte("i)"))
}

func printNil(w io.Writer) {
	w.Write([]byte("nil"))
}
