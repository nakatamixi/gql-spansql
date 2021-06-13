package converter

import (
	"regexp"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/vektah/gqlparser/v2/ast"
)

var spannerColumnRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]+$`)
var lowerRe = regexp.MustCompile(`^[a-z][a-zA-Z0-9]+$`)
var upperRe = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]+$`)

type Case int

const (
	SnakeCase Case = iota
	LowerCamelCase
	UpperCamelCase
	UnknownCase
	NoConvertCase
)

// LazySpannerColumnCaseOf jude case lazily.
func LazySpannerColumnCaseOf(s string) Case {
	if !spannerColumnRe.Match([]byte(s)) {
		return UnknownCase
	}
	if strings.Contains(s, "_") {
		return SnakeCase
	}
	if lowerRe.Match([]byte(s)) {
		return LowerCamelCase
	}
	if upperRe.Match([]byte(s)) {
		return UpperCamelCase
	}
	return UnknownCase
}

func DetectCase(def *ast.FieldDefinition) Case {
	return LazySpannerColumnCaseOf(def.Name)
}

func NormalizeCase(s string) string {
	return strcase.ToSnake(s)
}

func ConvertCase(s string, c Case) string {
	switch c {
	case NoConvertCase:
		return s
	case SnakeCase:
		return strcase.ToSnake(s)
	case LowerCamelCase:
		return strcase.ToLowerCamel(s)
	case UpperCamelCase:
		return strcase.ToCamel(s)
	default:
		return s
	}
	return s
}

func NewCase(c string) Case {
	switch c {
	case "snake":
		return SnakeCase
	case "lowercamel":
		return LowerCamelCase
	case "uppercamel":
		return UpperCamelCase
	case "":
		return NoConvertCase
	}
	return UnknownCase
}
