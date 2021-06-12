package converter

import (
	"strings"

	"github.com/iancoleman/strcase"

	"cloud.google.com/go/spanner/spansql"
	"github.com/vektah/gqlparser/v2/ast"
)

func DetectPK(objName string, fields ast.FieldList) ([]spansql.KeyPart, bool) {
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
