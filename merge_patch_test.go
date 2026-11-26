package main

import (
	"testing"
)

func TestParseMergePatches(t *testing.T) {
	merges, err := parseMergePatches("p", []interface{}{
		map[string]interface{}{"ts": float64(10), "v": float64(1), "p": "{}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if entry := merges[0].(map[string]interface{}); entry["ts"] != 10 || entry["v"] != 1 {
		t.Fatalf("ts/v not converted to int: %v", entry)
	}
	for name, bad := range map[string]interface{}{
		"not an array":     "oops",
		"entry not object": []interface{}{"oops"},
		"ts missing":       []interface{}{map[string]interface{}{"v": float64(1)}},
		"v not a number":   []interface{}{map[string]interface{}{"ts": float64(1), "v": "1"}},
	} {
		if _, err := parseMergePatches("p", bad); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
