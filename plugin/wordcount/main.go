package main

import (
	"strconv"
	"strings"
	"unicode"
)

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   string
	Value string
}

// Map maps input to key-value pairs.
func Map(filename string, contents string) []KeyValue {
	words := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r)
	})

	var res []KeyValue
	for _, w := range words {
		if w != "" {
			res = append(res, KeyValue{Key: strings.ToLower(w), Value: "1"})
		}
	}
	return res
}

// Reduce reduces multiple values for a key into a single value.
func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
