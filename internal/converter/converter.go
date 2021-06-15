package converter

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"

	"cloud.google.com/go/spanner/spansql"
	"github.com/vektah/gqlparser/v2/ast"
)

type Converter struct {
	schema                   *ast.Schema
	loose                    bool
	createdName, updatedName string
	tableCase, columnCase    Case
}

var (
	spanTypeRe = regexp.MustCompile(`^SpannerType: ?(.*)$`)
)

func NewConverter(s *ast.Schema, loose bool, createdName, updatedName string, tableCase, columnCase string) (*Converter, error) {
	tc := NewCase(tableCase)
	if tc == UnknownCase {
		return nil, fmt.Errorf("table case %s not found.", tableCase)
	}
	cc := NewCase(columnCase)
	if cc == UnknownCase {
		return nil, fmt.Errorf("column case %s not found.", columnCase)
	}
	return &Converter{
		schema:      s,
		loose:       loose,
		createdName: createdName,
		updatedName: updatedName,
		tableCase:   tc,
		columnCase:  cc,
	}, nil
}

func (c *Converter) SpannerSQL() (string, error) {
	sql := ""
	keys := make([]string, 0, len(c.schema.Types))
	for k := range c.schema.Types {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, name := range keys {
		t := c.schema.Types[name]
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
		Name: spansql.ID(ConvertCase(def.Name, c.tableCase)),
	}
	pk, found := c.DetectPK(def.Name, def.Fields)
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
	existsCreatedAt := false
	existsUpdatedAt := false
	for _, field := range def.Fields {
		col, err := c.ConvertField(field)
		if err != nil {
			return nil, err
		}

		sc.Columns = append(sc.Columns, *col)
		if c.createdName != "" && NormalizeCase(c.createdName) == NormalizeCase(field.Name) {
			existsCreatedAt = true
		}
		if c.updatedName != "" && NormalizeCase(c.updatedName) == NormalizeCase(field.Name) {
			existsUpdatedAt = true
		}
	}
	if !existsCreatedAt && c.createdName != "" {
		sc.Columns = append(sc.Columns, spansql.ColumnDef{
			Name: spansql.ID(c.createdName),
			Type: spansql.Type{
				Array: false,
				Base:  spansql.Timestamp,
			},
			NotNull: true,
		})
	}
	if !existsUpdatedAt && c.updatedName != "" {
		sc.Columns = append(sc.Columns, spansql.ColumnDef{
			Name: spansql.ID(c.updatedName),
			Type: spansql.Type{
				Array: false,
				Base:  spansql.Timestamp,
			},
			NotNull: true,
		})
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
	name, err := c.ConvertFieldName(f)
	if err != nil {
		return nil, err
	}
	return &spansql.ColumnDef{
		Name: spansql.ID(name),
		Type: spansql.Type{
			Array: isArray,
			Base:  typeBase,
			Len:   tlen,
		},
		NotNull: f.Type.NonNull,
	}, nil
}

func (c *Converter) ConvertFieldName(f *ast.FieldDefinition) (string, error) {
	namedType := f.Type.NamedType
	isArray := false
	if f.Type.NamedType == "" {
		isArray = true
		namedType = f.Type.Elem.NamedType
	}
	if def, ok := c.schema.Types[namedType]; ok {
		if def.Kind == "OBJECT" {
			fieldCase := c.columnCase
			if c.columnCase == NoConvertCase {
				// TODO best effort..
				fieldCase = DetectCase(f)
			}
			return ConvertCase(namedType+"Id"+addPluralSuffix(isArray), fieldCase), nil
		} else {
			return ConvertCase(f.Name, c.columnCase), nil
		}
	}
	return ConvertCase(f.Name, c.columnCase), nil
}

func addPluralSuffix(b bool) string {
	if b {
		return "s"
	}
	return ""
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
				desc := def.Description
				match := spanTypeRe.FindStringSubmatch(desc)
				if match == nil || len(match) <= 1 {
					return spansql.String, nil
				}
				if strings.Contains(match[1], "Int") {
					return spansql.Int64, nil
				}
				if strings.Contains(match[1], "ID") || strings.Contains(match[1], "String") {
					return spansql.String, nil
				}
				if strings.Contains(match[1], "Float") {
					return spansql.Float64, nil
				}
				if strings.Contains(match[1], "Boolean") {
					return spansql.Bool, nil
				}
			}

			if def.Kind == "OBJECT" {
				pk, found := c.DetectPK(def.Name, def.Fields)
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

func (c *Converter) DetectPK(objName string, fields ast.FieldList) ([]spansql.KeyPart, bool) {
	kp := []spansql.KeyPart{}
	found := false
	for _, f := range fields {
		desc := f.Description
		if strings.Contains(desc, "SpannerPK") {
			found = true
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(ConvertCase(f.Name, c.columnCase)),
			})
			continue
		}
		if NormalizeCase(f.Name) == NormalizeCase("Id") {
			found = true
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(ConvertCase(f.Name, c.columnCase)),
			})
			break
		}
		if NormalizeCase(f.Name) == NormalizeCase(objName+"Id") {
			found = true
			kp = append(kp, spansql.KeyPart{
				Column: spansql.ID(ConvertCase(f.Name, c.columnCase)),
			})
			break
		}

	}

	if !found {
		fieldCase := c.columnCase
		if c.columnCase == NoConvertCase {
			// TODO best effort..
			fieldCase = DetectCase(fields[0])
		}
		kp = append(kp, spansql.KeyPart{
			Column: spansql.ID(ConvertCase(objName+"Id", fieldCase)),
		})
	}
	return kp, found
}
