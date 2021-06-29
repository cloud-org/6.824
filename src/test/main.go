package main

import (
	"fmt"
	"strings"
	"unicode"
)

func main() {
	fmt.Println(strings.FieldsFunc("foo;   ; hello; 123;", func(r rune) bool {
		return !unicode.IsLetter(r)
	}))
}
