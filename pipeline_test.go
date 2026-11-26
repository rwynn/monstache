package main

import (
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/robertkrimen/otto"
)

// The global ("") pipeline script must be called with the real namespace.
func TestGlobalPipelineReceivesNamespace(t *testing.T) {
	saved, savedPlugin := pipeEnvs, pipePlugin
	t.Cleanup(func() { pipeEnvs, pipePlugin = saved, savedPlugin })
	pipePlugin = nil

	env := &executionEnv{VM: otto.New(), lock: &sync.Mutex{}}
	env.VM.Set("module", make(map[string]interface{}))
	if _, err := env.VM.Run(`module.exports = function(ns, changeEvent) { return [{ $match: { ns: ns } }]; }`); err != nil {
		t.Fatal(err)
	}
	pipeEnvs = map[string]*executionEnv{"": env}

	stages, err := buildPipe()("db.col", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(stages) != 1 {
		t.Fatalf("expected 1 stage, got %v", stages)
	}
	match, _ := stages[0].(map[string]interface{})["$match"].(map[string]interface{})
	if got := match["ns"]; got != "db.col" {
		t.Fatalf("global pipeline called with namespace %q, want %q", got, "db.col")
	}
}

// loadPipelines exits via log.Fatal, so these run their body in a child process.
func runPipelineConfigChild(t *testing.T) (string, bool) {
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), "MONSTACHE_PIPELINE_TEST_CHILD=1")
	out, err := cmd.CombinedOutput()
	return string(out), err == nil
}

const pipelineTestScript = `module.exports = function() { return []; }`

func TestDuplicatePipelinesRejected(t *testing.T) {
	if os.Getenv("MONSTACHE_PIPELINE_TEST_CHILD") == "1" {
		c := &configOptions{Pipeline: []javascript{
			{Namespace: "db.col", Script: pipelineTestScript},
			{Namespace: "db.col", Script: pipelineTestScript},
		}}
		c.loadPipelines()
		return
	}
	if out, ok := runPipelineConfigChild(t); ok || !strings.Contains(out, "Multiple pipelines") {
		t.Fatalf("two pipelines for one namespace were accepted:\n%s", out)
	}
}

func TestPipelineAllowedWhenFilterSharesNamespace(t *testing.T) {
	if os.Getenv("MONSTACHE_PIPELINE_TEST_CHILD") == "1" {
		c := &configOptions{
			Filter:   []javascript{{Namespace: "db.col", Script: `module.exports = function() { return true; }`}},
			Pipeline: []javascript{{Namespace: "db.col", Script: pipelineTestScript}},
		}
		c.loadFilters()
		c.loadPipelines()
		return
	}
	if out, ok := runPipelineConfigChild(t); !ok {
		t.Fatalf("a pipeline was rejected because a filter uses the same namespace:\n%s", out)
	}
}
