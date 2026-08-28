// gen-conformance reads prometheus/prometheus's promql/parser test
// corpus (parse_test.go) via go/ast and emits a JSON fixture that our
// Rust conformance harness consumes.
//
// Usage:
//
//	go run ./scripts/gen-conformance -in path/to/parse_test.go -out tests/fixtures/parse_test_corpus.json
//
// The tool is intentionally narrow: it extracts the `input string` and
// `fail bool` fields from the `testCases` slice literal in upstream's
// parse_test.go, skipping the expected-AST sub-trees. Expected-AST
// comparison would require a Go-side AST → canonical-JSON writer and
// a matching Rust reader — tracked as a follow-up. The input/fail
// subset still catches the majority of parser divergence (succeeds
// when upstream fails, or vice versa) and is enough to track the
// parser's conformance pass rate as grammar coverage grows.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"strconv"
)

type Case struct {
	Input  string `json:"input"`
	Fail   bool   `json:"fail"`
	ErrMsg string `json:"err_msg,omitempty"`
}

type Corpus struct {
	// Generated is a marker field so the JSON file is self-describing
	// as a produced artifact (JSON has no comment syntax). Complements
	// the `linguist-generated=true` entry in `.gitattributes`.
	Generated   string `json:"_generated"`
	UpstreamSha string `json:"upstream_sha"`
	Source      string `json:"source"`
	Cases       []Case `json:"cases"`
}

func main() {
	inPath := flag.String("in", "", "path to upstream promql/parser/parse_test.go")
	outPath := flag.String("out", "-", "output JSON path, '-' for stdout")
	sha := flag.String("sha", "", "upstream commit sha to record in the corpus header (optional)")
	flag.Parse()

	if *inPath == "" {
		log.Fatal("-in is required")
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, *inPath, nil, parser.AllErrors)
	if err != nil {
		log.Fatalf("parsing %s: %v", *inPath, err)
	}

	var cases []Case
	ast.Inspect(f, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		// Match the literal that initialises the test-cases slice.
		// Upstream writes it as `[]struct { input string; expected Expr;
		// fail bool; errMsg string }{...}`. The outer literal has that
		// exact type shape; each element is a nested struct literal.
		arr, ok := cl.Type.(*ast.ArrayType)
		if !ok {
			return true
		}
		st, ok := arr.Elt.(*ast.StructType)
		if !ok {
			return true
		}
		if !looksLikeTestCaseStruct(st) {
			return true
		}
		for _, elt := range cl.Elts {
			lit, ok := elt.(*ast.CompositeLit)
			if !ok {
				continue
			}
			if c, ok := extractCase(lit); ok {
				cases = append(cases, c)
			}
		}
		return false
	})

	out := Corpus{
		Generated:   "by scripts/gen-conformance — DO NOT EDIT",
		UpstreamSha: *sha,
		Source:      "prometheus/prometheus promql/parser/parse_test.go",
		Cases:       cases,
	}

	buf, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	buf = append(buf, '\n')

	if *outPath == "-" {
		if _, err := os.Stdout.Write(buf); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := os.WriteFile(*outPath, buf, 0o644); err != nil {
		log.Fatalf("write %s: %v", *outPath, err)
	}
	fmt.Fprintf(os.Stderr, "wrote %d cases to %s\n", len(cases), *outPath)
}

// looksLikeTestCaseStruct returns true when the struct literal has the
// fields we expect on upstream's test-case entries. Being field-name
// driven means we survive upstream reshufflings.
func looksLikeTestCaseStruct(st *ast.StructType) bool {
	needed := map[string]bool{"input": false, "fail": false}
	for _, field := range st.Fields.List {
		for _, name := range field.Names {
			if _, ok := needed[name.Name]; ok {
				needed[name.Name] = true
			}
		}
	}
	return needed["input"] && needed["fail"]
}

// extractCase pulls the `input`, `fail`, and `errMsg` fields out of a
// test-case composite literal. Returns `ok=false` when the literal
// lacks a non-empty `input` — those aren't runnable cases.
func extractCase(cl *ast.CompositeLit) (Case, bool) {
	var c Case
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		switch key.Name {
		case "input":
			if s, ok := stringLit(kv.Value); ok {
				c.Input = s
			}
		case "fail":
			if b, ok := boolLit(kv.Value); ok {
				c.Fail = b
			}
		case "errMsg":
			if s, ok := stringLit(kv.Value); ok {
				c.ErrMsg = s
			}
		}
	}
	if c.Input == "" {
		return c, false
	}
	return c, true
}

func stringLit(e ast.Expr) (string, bool) {
	bl, ok := e.(*ast.BasicLit)
	if !ok || bl.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(bl.Value)
	if err != nil {
		return "", false
	}
	return s, true
}

func boolLit(e ast.Expr) (bool, bool) {
	id, ok := e.(*ast.Ident)
	if !ok {
		return false, false
	}
	switch id.Name {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return false, false
}
