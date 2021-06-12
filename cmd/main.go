package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/nakatamixi/gql-spansql/internal/converter"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
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

	c := converter.NewConverter(schama, *loose)
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
