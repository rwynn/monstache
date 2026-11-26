package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// loadReplacements exits via log.Fatal, so the body runs in a child process.
func TestRelateWithoutNamespaceRejected(t *testing.T) {
	if os.Getenv("MONSTACHE_RELATE_TEST_CHILD") == "1" {
		c := &configOptions{Relate: []relation{{WithNamespace: "db.other"}}}
		c.loadReplacements()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRelateWithoutNamespaceRejected$")
	cmd.Env = append(os.Environ(), "MONSTACHE_RELATE_TEST_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "Relates must specify namespace and with-namespace") {
		t.Fatalf("relate without a namespace was accepted:\n%s", out)
	}
}
