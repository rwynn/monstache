package main

import (
	"math"
	"strings"
	"testing"
)

func TestExtractDataReportsMarshalError(t *testing.T) {
	// NaN can't be JSON encoded, so the error message can't include the doc
	_, err := extractData("missing", map[string]interface{}{"a": math.NaN()})
	if err == nil {
		t.Fatal("expected an error for a missing field")
	}
	if strings.Contains(err.Error(), "<nil>") {
		t.Fatalf("error detail is a nil error instead of the marshal error: %s", err)
	}
}

func TestParseElasticsearchVersionInvalidMajor(t *testing.T) {
	c := &configOptions{}
	if err := c.parseElasticsearchVersion("x.1"); err == nil {
		t.Fatal(`version "x.1" was accepted`)
	}
}
