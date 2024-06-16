package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/nktks/gql-spansql/internal/converter"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

type fschemas []string

func (s *fschemas) String() string {
	return fmt.Sprint(*s)
}

func (s *fschemas) Set(value string) error {
	if len(*s) > 0 {
		return errors.New("s flag already set")
	}
	for _, v := range strings.Split(value, ",") {
		*s = append(*s, v)
	}
	return nil
}

var (
	schemas     fschemas
	loose       = flag.Bool("loose", false, "loose type check.")
	createdName = flag.String("created-column-name", "", "if not empty, add this column as created_at Timestamp column.")
	updatedName = flag.String("updated-column-name", "", "if not empty, add this column as updated_at Timestamp column.")
	tableCase   = flag.String("table-case", "", "snake or lowercamel or uppercamel. if empty no convert.")
	columnCase  = flag.String("column-case", "", "snake or lowercamel or uppercamel. if empty no convert.")
)

func init() {
	flag.Var(&schemas, "s", "comma-separated path to input schema")
}

func main() {
	flag.Parse()
	var sources []*ast.Source
	if len(schemas) > 0 {
		var files []string
		for _, schema := range schemas {
			matches, err := filepath.Glob(schema)
			if err != nil {
				log.Fatalf("failed to glob schema filename %s: %w", schema, err)
			}
			for _, m := range matches {
				if has(files, m) {
					continue
				}
				files = append(files, m)
			}
		}
		for _, schema := range files {
			b, err := os.ReadFile(schema)
			if err != nil {
				log.Fatalf("Read from file failed: %v", err)
			}
			sources = append(sources, &ast.Source{Name: schema, Input: string(b)})
		}
	} else {
		b, err := readStdin()
		if err != nil {
			log.Fatalf("Read from stdin failed: %v", err)
		}
		sources = append(sources, &ast.Source{Input: string(b)})
	}
	schema, err := loadGQL(sources)
	if err != nil {
		log.Fatal(err)
	}

	c, err := converter.NewConverter(schema, *loose, *createdName, *updatedName, *tableCase, *columnCase)
	if err != nil {
		log.Fatal(err)
	}
	sql, err := c.SpannerSQL()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(sql)

}

func loadGQL(sources []*ast.Source) (*ast.Schema, error) {
	schema, err := gqlparser.LoadSchema(sources...)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func readStdin() ([]byte, error) {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return []byte{}, err
	}
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return []byte{}, err
		}
		return b, nil
	} else {
		return []byte{}, nil
	}
}

func has(ss []string, e string) bool {
	for _, s := range ss {
		if s == e {
			return true
		}
	}
	return false
}
