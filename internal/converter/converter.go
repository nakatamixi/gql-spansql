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

func (c *Converter) SpannerSQL() (string, error) {
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
		s, err := c.ConvertDefinition(t)
		if err != nil {
			return "", err
		}
		sql = sql + s.SQL() + ";\n"
	}
	return sql, nil
}
func (c *Converter) ConvertDefinition(def *ast.Definition) (*spansql.CreateTable, error) {
	sc := &spansql.CreateTable{
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
		col, err := c.ConvertField(field)
		if err != nil {
			return nil, err
		}

		sc.Columns = append(sc.Columns, *col)
	}
	return sc, nil
}
func (c *Converter) ConvertField(f *ast.FieldDefinition) (*spansql.ColumnDef, error) {
	isArray := false
	var typeBase spansql.TypeBase
	switch f.Type.NamedType {
	case "": // list
		isArray = true
		b, err := c.ConvertListField(f.Type.Elem)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f.Name, err)
		}
		typeBase = b
	default:
		b, err := c.ConvertType(f.Type.NamedType)
		if err != nil {
			return nil, err
		}
		typeBase = b
	}
	var tlen int64
	if typeBase == spansql.String {
		tlen = math.MaxInt64
	}
	return &spansql.ColumnDef{
		Name: spansql.ID(f.Name),
		Type: spansql.Type{
			Array: isArray,
			Base:  typeBase,
			Len:   tlen,
		},
		NotNull: f.Type.NonNull,
	}, nil
}

func (c *Converter) ConvertListField(l *ast.Type) (spansql.TypeBase, error) {
	if !l.NonNull && !c.loose {
		return 0, fmt.Errorf("spanner is not allowed null element in ARRAY.")
	}

	return c.ConvertType(l.NamedType)
}

func (c *Converter) ConvertType(t string) (spansql.TypeBase, error) {
	switch t {
	case "Int":
		return spansql.Int64, nil
	case "ID", "String":
		return spansql.String, nil
	case "Float":
		return spansql.Float64, nil
	case "Boolean":
		return spansql.Bool, nil
	// TODO this is custom scalar type
	case "Time", "TimeStamp", "Timestamp":
		return spansql.Timestamp, nil
	// TODO this is custom scalar type
	case "Date":
		return spansql.Date, nil
	default:
		if def, ok := c.schema.Types[t]; ok {
			if def.Kind == "ENUM" {
				return spansql.Int64, nil
			}
			if def.Kind == "SCALAR" {
				// TODO definie custom spanner type
				desc := def.Description
				if desc == "" {
					return spansql.String, nil
				}
				if strings.Contains(desc, "Int") {
					return spansql.Int64, nil
				}
				if strings.Contains(desc, "ID") || strings.Contains(desc, "String") {
					return spansql.String, nil
				}
				if strings.Contains(desc, "Float") {
					return spansql.Float64, nil
				}
				if strings.Contains(desc, "Boolean") {
					return spansql.Bool, nil
				}
			}

			// TODO this case must change column name to xxxID
			if def.Kind == "OBJECT" {
				pk, found := DetectPK(def.Name, def.Fields)
				if !found {
					return spansql.String, nil
				}
				if len(pk) > 1 {
					return 0, fmt.Errorf("relation to multiple pk keys is not supported. %s", t)
				}
				for _, f := range def.Fields {
					if string(pk[0].Column) == f.Name {
						col, err := c.ConvertField(f)
						if err != nil {
							return 0, err
						}
						return col.Type.Base, nil
					}
				}
				return spansql.String, nil
			}
		}
	}
	return 0, fmt.Errorf("scalar type %s is not found.", t)

}
