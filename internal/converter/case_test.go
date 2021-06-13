package converter_test

import (
	"testing"

	"github.com/nakatamixi/gql-spansql/internal/converter"
	"github.com/stretchr/testify/require"
)

func TestSpannerColumnLazySpannerColumnCaseOf(t *testing.T) {
	require.Equal(t, converter.UnknownCase, converter.LazySpannerColumnCaseOf("kebab-case"))
	require.Equal(t, converter.SnakeCase, converter.LazySpannerColumnCaseOf("snake_case"))
	require.Equal(t, converter.SnakeCase, converter.LazySpannerColumnCaseOf("Snake_case"))
	require.Equal(t, converter.UnknownCase, converter.LazySpannerColumnCaseOf("kebab-mixed-Snake_case"))
	require.Equal(t, converter.LowerCamelCase, converter.LazySpannerColumnCaseOf("lowerCamelCase"))
	require.Equal(t, converter.LowerCamelCase, converter.LazySpannerColumnCaseOf("notlowercamelcase")) // lazy check...
	require.Equal(t, converter.UpperCamelCase, converter.LazySpannerColumnCaseOf("UpperCamelCase"))
	require.Equal(t, converter.UpperCamelCase, converter.LazySpannerColumnCaseOf("Notuppercamelcase")) // lazy check...
}

func TestConvertCase(t *testing.T) {
	require.Equal(t, "snake_case", converter.ConvertCase("snake_case", converter.NoConvertCase))
	require.Equal(t, "snake_case", converter.ConvertCase("snake_case", converter.SnakeCase))
	require.Equal(t, "snakeCase", converter.ConvertCase("snake_case", converter.LowerCamelCase))
	require.Equal(t, "SnakeCase", converter.ConvertCase("snake_case", converter.UpperCamelCase))
}
