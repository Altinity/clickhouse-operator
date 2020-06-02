
# go-intersect

  Find the intersection of two iterable values.

  This library provides multiple implementations which each have their strong and weak points.

  Read the [docs](http://godoc.org/github.com/juliangruber/go-intersect).

## Installation

```bash
$ go get github.com/juliangruber/go-intersect
```

## Example

```go
import "github.com/juliangruber/go-intersect"
import "fmt"

func main() {
  a := []int{1, 2, 3}
  b := []int{2, 3, 4}
  fmt.Println(Intersect.Simple(a, b))
}
```

