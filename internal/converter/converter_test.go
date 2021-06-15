package converter_test

import (
	_ "embed"
	"testing"

	"cloud.google.com/go/spanner/spansql"
	"github.com/nakatamixi/gql-spansql/internal/converter"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestConverter_NewConverter(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		_, err := converter.NewConverter(&ast.Schema{}, true, "", "", "", "")
		require.NoError(t, err)
		_, err = converter.NewConverter(&ast.Schema{}, true, "", "", "snake", "")
		require.NoError(t, err)
		_, err = converter.NewConverter(&ast.Schema{}, true, "", "", "lowercamel", "")
		require.NoError(t, err)
		_, err = converter.NewConverter(&ast.Schema{}, true, "", "", "uppercamel", "")
		require.NoError(t, err)
		_, err = converter.NewConverter(&ast.Schema{}, true, "", "", "", "snake")
		require.NoError(t, err)
		_, err = converter.NewConverter(&ast.Schema{}, true, "", "", "", "lowercamel")
		require.NoError(t, err)
		_, err = converter.NewConverter(&ast.Schema{}, true, "", "", "", "uppercamel")
		require.NoError(t, err)
	})
	t.Run("invalid case", func(t *testing.T) {
		_, err := converter.NewConverter(&ast.Schema{}, true, "", "", "invalid", "")
		require.Error(t, err)
		_, err = converter.NewConverter(&ast.Schema{}, true, "", "", "", "invalid")
		require.Error(t, err)
	})
}

//go:embed testdata/spanner_sql.gql
var spannerSQLBody []byte

func TestConverter_SpannerSQL(t *testing.T) {
	s, err := loadGQL(spannerSQLBody)
	require.NoError(t, err)
	c, err := converter.NewConverter(s, true, "", "", "", "")
	require.NoError(t, err)
	sql, err := c.SpannerSQL()
	require.NoError(t, err)
	require.Equal(t, `CREATE TABLE Item (
  itemId STRING(MAX) NOT NULL,
) PRIMARY KEY(itemId);
CREATE TABLE User (
  userId STRING(MAX) NOT NULL,
  state INT64 NOT NULL,
  time TIMESTAMP NOT NULL,
) PRIMARY KEY(userId);
`, sql)
}

//go:embed testdata/convert_definition.gql
var convertDefinitionBody []byte

func TestConverter_ConvertDefinition(t *testing.T) {
	s, err := loadGQL(convertDefinitionBody)
	require.NoError(t, err)
	t.Run("table case", func(t *testing.T) {
		t.Run("no convert", func(t *testing.T) {
			c, err := converter.NewConverter(s, true, "", "", "", "")
			require.NoError(t, err)
			createTable, err := c.ConvertDefinition(s.Types["User"])
			require.NoError(t, err)
			require.Equal(t, "User", string(createTable.Name))
		})
		t.Run("convert", func(t *testing.T) {
			c, err := converter.NewConverter(s, true, "", "", "snake", "")
			require.NoError(t, err)
			createTable, err := c.ConvertDefinition(s.Types["User"])
			require.NoError(t, err)
			require.Equal(t, "user", string(createTable.Name))
		})
	})
	t.Run("detect pk", func(t *testing.T) {
		t.Run("has object id column", func(t *testing.T) {
			c, err := converter.NewConverter(s, true, "", "", "", "")
			require.NoError(t, err)
			createTable, err := c.ConvertDefinition(s.Types["User"])
			require.NoError(t, err)
			require.Equal(t, "userId", string(createTable.PrimaryKey[0].Column))
		})
		t.Run("has id column", func(t *testing.T) {
			c, err := converter.NewConverter(s, true, "", "", "", "")
			require.NoError(t, err)
			createTable, err := c.ConvertDefinition(s.Types["HasId"])
			require.NoError(t, err)
			require.Equal(t, "id", string(createTable.PrimaryKey[0].Column))
		})
		t.Run("has no id column", func(t *testing.T) {
			c, err := converter.NewConverter(s, true, "", "", "", "")
			require.NoError(t, err)
			createTable, err := c.ConvertDefinition(s.Types["HasNoId"])
			require.NoError(t, err)
			require.Equal(t, "hasNoIdId", string(createTable.PrimaryKey[0].Column))
		})
		t.Run("has description column", func(t *testing.T) {
			c, err := converter.NewConverter(s, true, "", "", "", "")
			require.NoError(t, err)
			createTable, err := c.ConvertDefinition(s.Types["HasDescription"])
			require.NoError(t, err)
			require.Equal(t, "pk", string(createTable.PrimaryKey[0].Column))
		})
	})
	t.Run("created column", func(t *testing.T) {
		t.Run("has same name column", func(t *testing.T) {
			c, err := converter.NewConverter(s, true, "createdAt", "", "", "")
			require.NoError(t, err)
			createTable, err := c.ConvertDefinition(s.Types["HasSameColumn"])
			require.NoError(t, err)
			require.Equal(t, `CREATE TABLE HasSameColumn (
  hasSameColumnId STRING(MAX),
  createdAt TIMESTAMP NOT NULL,
  updatedAt TIMESTAMP NOT NULL,
) PRIMARY KEY(hasSameColumnId)`, createTable.SQL())
		})
		t.Run("has no same name column", func(t *testing.T) {
			c, err := converter.NewConverter(s, true, "createdAt", "", "", "")
			require.NoError(t, err)
			createTable, err := c.ConvertDefinition(s.Types["HasNoSameColumn"])
			require.NoError(t, err)
			require.Equal(t, `CREATE TABLE HasNoSameColumn (
  hasNoSameColumnId STRING(MAX),
  state INT64 NOT NULL,
  createdAt TIMESTAMP NOT NULL,
) PRIMARY KEY(hasNoSameColumnId)`, createTable.SQL())
		})
	})
	t.Run("updated column", func(t *testing.T) {
		t.Run("has same name column", func(t *testing.T) {
			c, err := converter.NewConverter(s, true, "updatedAt", "", "", "")
			require.NoError(t, err)
			createTable, err := c.ConvertDefinition(s.Types["HasSameColumn"])
			require.NoError(t, err)
			require.Equal(t, `CREATE TABLE HasSameColumn (
  hasSameColumnId STRING(MAX),
  createdAt TIMESTAMP NOT NULL,
  updatedAt TIMESTAMP NOT NULL,
) PRIMARY KEY(hasSameColumnId)`, createTable.SQL())
		})
		t.Run("has no same name column", func(t *testing.T) {
			c, err := converter.NewConverter(s, true, "updatedAt", "", "", "")
			require.NoError(t, err)
			createTable, err := c.ConvertDefinition(s.Types["HasNoSameColumn"])
			require.NoError(t, err)
			require.Equal(t, `CREATE TABLE HasNoSameColumn (
  hasNoSameColumnId STRING(MAX),
  state INT64 NOT NULL,
  updatedAt TIMESTAMP NOT NULL,
) PRIMARY KEY(hasNoSameColumnId)`, createTable.SQL())
		})
	})
}

//go:embed testdata/convert_field.gql
var convertFieldBody []byte

func TestConverter_ConvertField(t *testing.T) {
	s, err := loadGQL(convertFieldBody)
	require.NoError(t, err)
	t.Run("list", func(t *testing.T) {
		t.Run("nonnull list", func(t *testing.T) {
			t.Run("nonnull element", func(t *testing.T) {
				c, err := converter.NewConverter(s, true, "", "", "", "")
				require.NoError(t, err)
				column, err := c.ConvertField(s.Types["User"].Fields.ForName("nonNullListOfNonNullState"))
				require.NoError(t, err)
				require.Equal(t, "nonNullListOfNonNullState ARRAY<INT64> NOT NULL", column.SQL())
			})
			t.Run("null element, loose case", func(t *testing.T) {
				c, err := converter.NewConverter(s, true, "", "", "", "")
				require.NoError(t, err)
				column, err := c.ConvertField(s.Types["User"].Fields.ForName("nonNullListOfNullState"))
				require.NoError(t, err)
				require.Equal(t, "nonNullListOfNullState ARRAY<INT64> NOT NULL", column.SQL())
			})
			t.Run("null element, not loose case", func(t *testing.T) {
				c, err := converter.NewConverter(s, false, "", "", "", "")
				require.NoError(t, err)
				_, err = c.ConvertField(s.Types["User"].Fields.ForName("nonNullListOfNullState"))
				require.Error(t, err)
			})
		})
		t.Run("null list", func(t *testing.T) {
			t.Run("nonnull element", func(t *testing.T) {
				c, err := converter.NewConverter(s, true, "", "", "", "")
				require.NoError(t, err)
				column, err := c.ConvertField(s.Types["User"].Fields.ForName("nullListOfNonNullState"))
				require.NoError(t, err)
				require.Equal(t, "nullListOfNonNullState ARRAY<INT64>", column.SQL())
			})
			t.Run("null element, loose case", func(t *testing.T) {
				c, err := converter.NewConverter(s, true, "", "", "", "")
				require.NoError(t, err)
				column, err := c.ConvertField(s.Types["User"].Fields.ForName("nullListOfNullState"))
				require.NoError(t, err)
				require.Equal(t, "nullListOfNullState ARRAY<INT64>", column.SQL())
			})
			t.Run("null element, not loose case", func(t *testing.T) {
				c, err := converter.NewConverter(s, false, "", "", "", "")
				require.NoError(t, err)
				_, err = c.ConvertField(s.Types["User"].Fields.ForName("nullListOfNullState"))
				require.Error(t, err)
			})
		})
	})
	t.Run("named type", func(t *testing.T) {
		c, err := converter.NewConverter(s, true, "", "", "", "")
		require.NoError(t, err)
		column, err := c.ConvertField(s.Types["User"].Fields.ForName("userId"))
		require.NoError(t, err)
		require.Equal(t, "userId STRING(MAX) NOT NULL", column.SQL())
	})
}

//go:embed testdata/convert_field_name.gql
var convertFieldNameBody []byte

func TestConverter_ConvertFieldName(t *testing.T) {
	s, err := loadGQL(convertFieldNameBody)
	require.NoError(t, err)
	c, err := converter.NewConverter(s, true, "", "", "", "")
	require.NoError(t, err)
	t.Run("field does not array", func(t *testing.T) {
		t.Run("field is object", func(t *testing.T) {
			name, err := c.ConvertFieldName(s.Types["User"].Fields.ForName("aItem"))
			require.NoError(t, err)
			require.Equal(t, "aItemId", name)
		})
		t.Run("field does not object", func(t *testing.T) {
			name, err := c.ConvertFieldName(s.Types["User"].Fields.ForName("userId"))
			require.NoError(t, err)
			require.Equal(t, "userId", name)
		})
	})
	t.Run("field is array", func(t *testing.T) {
		t.Run("field element is object", func(t *testing.T) {
			name, err := c.ConvertFieldName(s.Types["User"].Fields.ForName("aItems"))
			require.NoError(t, err)
			require.Equal(t, "aItemIds", name)
		})
		t.Run("field element does not object", func(t *testing.T) {
			name, err := c.ConvertFieldName(s.Types["User"].Fields.ForName("userIds"))
			require.NoError(t, err)
			require.Equal(t, "userIds", name)
		})
	})
}

//go:embed testdata/convert_type.gql
var convertTypeBody []byte

func TestConverter_ConvertType(t *testing.T) {
	s, err := loadGQL(convertTypeBody)
	require.NoError(t, err)
	t.Run("ID", func(t *testing.T) {
		c, err := converter.NewConverter(s, true, "", "", "", "")
		require.NoError(t, err)
		typeBase, err := c.ConvertType(s.Types["User"].Fields.ForName("id").Type.NamedType)
		require.NoError(t, err)
		require.Equal(t, spansql.String, typeBase)
	})
	t.Run("Int", func(t *testing.T) {
		c, err := converter.NewConverter(s, true, "", "", "", "")
		require.NoError(t, err)
		typeBase, err := c.ConvertType(s.Types["User"].Fields.ForName("int").Type.NamedType)
		require.NoError(t, err)
		require.Equal(t, spansql.Int64, typeBase)
	})
	t.Run("Float", func(t *testing.T) {
		c, err := converter.NewConverter(s, true, "", "", "", "")
		require.NoError(t, err)
		typeBase, err := c.ConvertType(s.Types["User"].Fields.ForName("float").Type.NamedType)
		require.NoError(t, err)
		require.Equal(t, spansql.Float64, typeBase)
	})
	t.Run("Boolean", func(t *testing.T) {
		c, err := converter.NewConverter(s, true, "", "", "", "")
		require.NoError(t, err)
		typeBase, err := c.ConvertType(s.Types["User"].Fields.ForName("boolean").Type.NamedType)
		require.NoError(t, err)
		require.Equal(t, spansql.Bool, typeBase)
	})
	t.Run("Enum", func(t *testing.T) {
		c, err := converter.NewConverter(s, true, "", "", "", "")
		require.NoError(t, err)
		typeBase, err := c.ConvertType(s.Types["User"].Fields.ForName("enum").Type.NamedType)
		require.NoError(t, err)
		require.Equal(t, spansql.Int64, typeBase)
	})
	t.Run("scalar default", func(t *testing.T) {
		c, err := converter.NewConverter(s, true, "", "", "", "")
		require.NoError(t, err)
		typeBase, err := c.ConvertType(s.Types["User"].Fields.ForName("scalarDefault").Type.NamedType)
		require.NoError(t, err)
		require.Equal(t, spansql.String, typeBase)
	})
	t.Run("scalar with description", func(t *testing.T) {
		c, err := converter.NewConverter(s, true, "", "", "", "")
		require.NoError(t, err)
		typeBase, err := c.ConvertType(s.Types["User"].Fields.ForName("scalarWithDesc").Type.NamedType)
		require.NoError(t, err)
		require.Equal(t, spansql.Int64, typeBase)
	})
	t.Run("object cant detect pk", func(t *testing.T) {
		c, err := converter.NewConverter(s, true, "", "", "", "")
		require.NoError(t, err)
		typeBase, err := c.ConvertType(s.Types["User"].Fields.ForName("objectCantDetectPK").Type.NamedType)
		require.NoError(t, err)
		require.Equal(t, spansql.String, typeBase)
	})
	t.Run("object can detect pk", func(t *testing.T) {
		c, err := converter.NewConverter(s, true, "", "", "", "")
		require.NoError(t, err)
		typeBase, err := c.ConvertType(s.Types["User"].Fields.ForName("objectCanDetectPK").Type.NamedType)
		require.NoError(t, err)
		require.Equal(t, spansql.Int64, typeBase)
	})
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
