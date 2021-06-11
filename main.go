package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"strings"

	"cloud.google.com/go/spanner/spansql"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/iancoleman/strcase"
)

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
	schama, err := loadGQL(body)
	if err != nil {
		log.Fatal(err)
	}

	for name, t := range schama.Types {
		if t.BuiltIn {
			continue
		}
		if t.Kind != "OBJECT" {
			continue
		}
		if name == "Query" || name == "Mutation" || name == "Subscription" {
			continue
		}
		c := convertObject(t, schama.Types, *loose)
		fmt.Println(c.SQL() + ";")
	}

}

func loadGQL(b []byte) (*ast.Schema, error) {
	schama, err := gqlparser.LoadSchema(&ast.Source{
		Input: string(b),
	})
	if err != nil {
		return nil, err
	}
	return schama, nil
}

func convertObject(obj *ast.Definition, types map[string]*ast.Definition, loose bool) spansql.CreateTable {
	c := spansql.CreateTable{
		Name: spansql.ID(obj.Name),
	}
	pk, found := detectPK(obj.Name, obj.Fields)
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
		c.Columns = append(c.Columns, convertField(field, types, loose))
	}
	return c
}

func convertField(f *ast.FieldDefinition, types map[string]*ast.Definition, loose bool) spansql.ColumnDef {
	isArray := false
	var typeBase spansql.TypeBase
	switch f.Type.NamedType {
	case "": // list
		isArray = true
		b, err := convertListField(f.Type.Elem, types, loose)
		if err != nil {
			panic(fmt.Errorf("%s: %w", f.Name, err))
		}
		typeBase = b
	default:
		typeBase = convertType(f.Type.NamedType, types, loose)
	}
	var tlen int64
	if typeBase == spansql.String {
		tlen = math.MaxInt64
	}
	return spansql.ColumnDef{
		Name: spansql.ID(f.Name),
		Type: spansql.Type{
			Array: isArray,
			Base:  typeBase,
			Len:   tlen,
		},
		NotNull: f.Type.NonNull,
	}
}

func convertListField(l *ast.Type, types map[string]*ast.Definition, loose bool) (spansql.TypeBase, error) {
	if !l.NonNull && !loose {
		return 0, fmt.Errorf("spanner is not allowed null element in ARRAY.")
	}

	return convertType(l.NamedType, types, loose), nil
}

func convertType(t string, types map[string]*ast.Definition, loose bool) spansql.TypeBase {
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
		if def, ok := types[t]; ok {
			if def.Kind == "ENUM" {
				return spansql.Int64
			}
			if def.Kind == "SCALAR" {
				// TODO definie custom spanner type
				desc := def.Description
				if desc == "" {
					return spansql.String
				}
				if strings.Contains(desc, "Int") {
					return spansql.Int64
				}
				if strings.Contains(desc, "ID") || strings.Contains(desc, "String") {
					return spansql.String
				}
				if strings.Contains(desc, "Float") {
					return spansql.Float64
				}
				if strings.Contains(desc, "Boolean") {
					return spansql.Bool
				}
			}

			// TODO this case must change column name to xxxID
			if def.Kind == "OBJECT" {
				pk, found := detectPK(def.Name, def.Fields)
				if !found {
					return spansql.String
				}
				if len(pk) > 1 {
					panic(fmt.Errorf("relation to multiple pk keys is not supported. %s", t))
				}
				for _, f := range def.Fields {
					if string(pk[0].Column) == f.Name {
						return convertField(f, types, loose).Type.Base
					}
				}
				return spansql.String
			}
		}
	}
	panic(fmt.Sprintf("scalar type %s is not found.", t))
	return 0

}

func detectPK(objName string, fields ast.FieldList) ([]spansql.KeyPart, bool) {
	kp := []spansql.KeyPart{}
	found := false
	for _, f := range fields {
		desc := f.Description
		if strings.Contains(desc, "SpannerPK") {
			found = true
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(f.Name),
			})
			continue
		}
		if normalize(f.Name) == "Id" {
			found = true
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(f.Name),
			})
			break
		}
		if normalize(f.Name) == normalize(objName+"ID") {
			found = true
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(f.Name),
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
