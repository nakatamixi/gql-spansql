package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"strings"

	"cloud.google.com/go/spanner/spansql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"

	"github.com/iancoleman/strcase"
)

type defs map[string]interface{}

func (d defs) IsObject(s string) bool {
	if v, ok := d[s]; ok {
		_, b := v.(*ast.ObjectDefinition)
		return b
	}
	return false
}

func (d defs) IsEnum(s string) bool {
	if v, ok := d[s]; ok {
		_, b := v.(*ast.EnumDefinition)
		return b
	}
	return false
}

func (d defs) IsScalar(s string) bool {
	if v, ok := d[s]; ok {
		_, b := v.(*ast.ScalarDefinition)
		return b
	}
	return false
}

var (
	schama = flag.String("s", "", "path to input schama")
	loose  = flag.Bool("loose", false, "loose type check.")
)

func main() {
	flag.Parse()
	body, err := ioutil.ReadFile(*schama)
	if err != nil {
		log.Fatalf("Read from file failed: %v", err)
	}
	astDoc := parse(string(body))

	defs := map[string]interface{}{}
	for _, node := range astDoc.Definitions {
		switch n := node.(type) {
		case *ast.ObjectDefinition:
			if n.GetName().Value == "Query" || n.GetName().Value == "Mutation" || n.GetName().Value == "Subscription" {
				continue
			}
			defs[n.GetName().Value] = n
		case *ast.EnumDefinition:
			defs[n.GetName().Value] = n
		case *ast.ScalarDefinition:
			defs[n.GetName().Value] = n
		}
	}
	for _, node := range defs {
		switch n := node.(type) {
		case *ast.ObjectDefinition:
			c := convertObject(n, defs, *loose)
			fmt.Println(c.SQL() + ";")
		}
	}

}

func parse(query string) *ast.Document {
	astDoc, err := parser.Parse(parser.ParseParams{
		Source: query,
		Options: parser.ParseOptions{
			NoLocation: false,
			NoSource:   true,
		},
	})
	if err != nil {
		log.Fatalf("Parse failed: %v", err)
	}
	return astDoc
}

func convertObject(obj *ast.ObjectDefinition, d defs, loose bool) spansql.CreateTable {
	c := spansql.CreateTable{
		Name: spansql.ID(obj.GetName().Value),
	}
	pk, found := detectPK(obj.GetName().Value, obj.Fields)
	c.PrimaryKey = pk
	if !found {
		c.Columns = append(c.Columns, spansql.ColumnDef{
			Name: pk[0].Column,
			Type: spansql.Type{
				Array: false,
				Base:  spansql.String,
				Len:   math.MaxInt64,
			},
			NotNull: false,
		})
	}
	for _, field := range obj.Fields {
		c.Columns = append(c.Columns, convertField(field, d, loose))
	}
	return c
}

func convertField(field *ast.FieldDefinition, d defs, loose bool) spansql.ColumnDef {
	isArray := false
	nonNull := false
	var typeBase spansql.TypeBase
	switch f := field.Type.(type) {
	case *ast.List:
		isArray = true
		b, err := convertListField(f, d, loose)
		if err != nil {
			panic(fmt.Errorf("%s: %w", field.Name.Value, err))
		}
		typeBase = b
	case *ast.NonNull:
		nonNull = true
		switch nf := f.Type.(type) {
		case *ast.List:
			isArray = true
			b, err := convertListField(nf, d, loose)
			if err != nil {
				panic(fmt.Errorf("%s: %w", field.Name.Value, err))
			}
			typeBase = b
		case *ast.Named:
			typeBase = convertType(nf.Name.Value, d, loose)
		}
	case *ast.Named:
		typeBase = convertType(f.Name.Value, d, loose)
	}
	var tlen int64
	if typeBase == spansql.String {
		tlen = math.MaxInt64
	}
	return spansql.ColumnDef{
		Name: spansql.ID(field.Name.Value),
		Type: spansql.Type{
			Array: isArray,
			Base:  typeBase,
			Len:   tlen,
		},
		NotNull: nonNull,
	}
}

func convertListField(l *ast.List, d defs, loose bool) (spansql.TypeBase, error) {
	switch t := l.Type.(type) {
	case *ast.NonNull:
		if named, ok := t.Type.(*ast.Named); ok {
			return convertType(named.Name.Value, d, loose), nil
		}
	case *ast.Named:
		if !loose {
			return 0, fmt.Errorf("spanner is not allowed null element in ARRAY.")
		}
		return convertType(t.Name.Value, d, loose), nil
	}
	return 0, fmt.Errorf("TODO exist not named in list")
}

func convertType(t string, d defs, loose bool) spansql.TypeBase {
	switch t {
	case "Int":
		return spansql.Int64
	case "ID", "String":
		return spansql.String
	case "Float":
		return spansql.Float64
	case "Boolean":
		return spansql.Bool
	// TODO this is custom scalar type
	case "Time", "TimeStamp", "Timestamp":
		return spansql.Timestamp
	// TODO this is custom scalar type
	case "Date":
		return spansql.Date
	default:
		if d.IsEnum(t) {
			return spansql.Int64
		}
		if d.IsScalar(t) {
			if scalar, ok := d[t].(*ast.ScalarDefinition); ok {
				desc := scalar.GetDescription()
				if desc == nil {
					return spansql.String
				}
				if strings.Contains(desc.Value, "Int") {
					return spansql.Int64
				}
				if strings.Contains(desc.Value, "ID") || strings.Contains(desc.Value, "String") {
					return spansql.String
				}
				if strings.Contains(desc.Value, "Float") {
					return spansql.Float64
				}
				if strings.Contains(desc.Value, "Boolean") {
					return spansql.Bool
				}
			}
			return spansql.String

		}
		// TODO this case must change column name to xxxID
		if d.IsObject(t) {
			if obj, ok := d[t].(*ast.ObjectDefinition); ok {
				pk, found := detectPK(obj.GetName().Value, obj.Fields)
				if !found {
					return spansql.String
				}
				if len(pk) > 1 {
					panic(fmt.Errorf("relation to multiple pk keys is not supported. %s", t))
				}
				for _, f := range obj.Fields {
					if string(pk[0].Column) == f.Name.Value {
						return convertField(f, d, loose).Type.Base
					}
				}
			}
			return spansql.String
		}
	}
	panic(fmt.Sprintf("scalar type %s is not found.", t))
	return 0

}

func detectPK(objName string, fields []*ast.FieldDefinition) ([]spansql.KeyPart, bool) {
	kp := []spansql.KeyPart{}
	found := false
	for _, f := range fields {
		desc := f.GetDescription()
		if desc != nil && strings.Contains(desc.Value, "pk") {
			found = true
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(f.Name.Value),
			})
			continue
		}
		if normalize(f.Name.Value) == "Id" {
			found = true
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(f.Name.Value),
			})
			break
		}
		if normalize(f.Name.Value) == normalize(objName+"ID") {
			found = true
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(f.Name.Value),
			})
			break
		}

	}

	if !found {
		if strings.Contains(objName, "_") {
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(objName + "_id"),
			})
		} else {
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(objName + "ID"),
			})
		}
	}
	return kp, found
}

func normalize(s string) string {
	return strcase.ToCamel(s)
}
