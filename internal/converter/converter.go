package converter

import (
	"fmt"
	"math"
	"strings"

	"cloud.google.com/go/spanner/spansql"
	"github.com/vektah/gqlparser/v2/ast"
)

type Converter struct {
	schema *ast.Schema
	loose  bool
}

func NewConverter(s *ast.Schema, loose bool) *Converter {
	return &Converter{
		schema: s,
		loose:  loose,
	}
}

func (c *Converter) SpannerSQL() string {
	sql := ""
	for name, t := range c.schema.Types {
		if t.BuiltIn {
			continue
		}
		if t.Kind != "OBJECT" {
			continue
		}
		if name == "Query" || name == "Mutation" || name == "Subscription" {
			continue
		}
		s := c.ConvertDefinition(t)
		sql = sql + s.SQL() + ";\n"
	}
	return sql
}
func (c *Converter) ConvertDefinition(def *ast.Definition) spansql.CreateTable {
	sc := spansql.CreateTable{
		Name: spansql.ID(def.Name),
	}
	pk, found := DetectPK(def.Name, def.Fields)
	sc.PrimaryKey = pk
	if !found {
		sc.Columns = append(sc.Columns, spansql.ColumnDef{
			Name: pk[0].Column,
			Type: spansql.Type{
				Array: false,
				Base:  spansql.String,
				Len:   math.MaxInt64,
			},
			NotNull: false,
		})
	}
	for _, field := range def.Fields {
		sc.Columns = append(sc.Columns, c.ConvertField(field))
	}
	return sc
}
func (c *Converter) ConvertField(f *ast.FieldDefinition) spansql.ColumnDef {
	isArray := false
	var typeBase spansql.TypeBase
	switch f.Type.NamedType {
	case "": // list
		isArray = true
		b, err := c.ConvertListField(f.Type.Elem)
		if err != nil {
			panic(fmt.Errorf("%s: %w", f.Name, err))
		}
		typeBase = b
	default:
		typeBase = c.ConvertType(f.Type.NamedType)
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

func (c *Converter) ConvertListField(l *ast.Type) (spansql.TypeBase, error) {
	if !l.NonNull && !c.loose {
		return 0, fmt.Errorf("spanner is not allowed null element in ARRAY.")
	}

	return c.ConvertType(l.NamedType), nil
}

func (c *Converter) ConvertType(t string) spansql.TypeBase {
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
		if def, ok := c.schema.Types[t]; ok {
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
				pk, found := DetectPK(def.Name, def.Fields)
				if !found {
					return spansql.String
				}
				// TODO err
				if len(pk) > 1 {
					panic(fmt.Errorf("relation to multiple pk keys is not supported. %s", t))
				}
				for _, f := range def.Fields {
					if string(pk[0].Column) == f.Name {
						return c.ConvertField(f).Type.Base
					}
				}
				return spansql.String
			}
		}
	}
	panic(fmt.Sprintf("scalar type %s is not found.", t))
	return 0

}
