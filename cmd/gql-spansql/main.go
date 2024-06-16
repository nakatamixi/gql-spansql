package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/nktks/gql-spansql/internal/converter"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	schama      = flag.String("s", "", "path to input schama")
	loose       = flag.Bool("loose", false, "loose type check.")
	createdName = flag.String("created-column-name", "", "if not empty, add this column as created_at Timestamp column.")
	updatedName = flag.String("updated-column-name", "", "if not empty, add this column as updated_at Timestamp column.")
	tableCase   = flag.String("table-case", "", "snake or lowercamel or uppercamel. if empty no convert.")
	columnCase  = flag.String("column-case", "", "snake or lowercamel or uppercamel. if empty no convert.")
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

	c, err := converter.NewConverter(schama, *loose, *createdName, *updatedName, *tableCase, *columnCase)
	if err != nil {
		log.Fatal(err)
	}
	sql, err := c.SpannerSQL()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(sql)

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
