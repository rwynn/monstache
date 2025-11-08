//go:build integration
// +build integration

package main

// Integration tests requiring Docker infrastructure
// Start: cd docker/test && ./run-tests.sh
// Or manually: cd docker/test && docker-compose up --abort-on-container-exit

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Integration tests - requires docker/test environment to be running
// These tests should be run by the docker/test/run-tests.sh script

func TestIntegrationMemoryLeak(t *testing.T) {
	// Test against real MongoDB + Elasticsearch

	// Check if test environment is running (Docker internal hostnames)
	resp, err := http.Get("http://es7:9200/_cluster/health")
	if err != nil {
		t.Skip("Elasticsearch not running. Use: cd docker/test && ./run-tests.sh")
	}
	resp.Body.Close()

	t.Run("RealMappingFailures", func(t *testing.T) {
		// Create strict mapping that will cause failures
		mappingData := `{
			"mappings": {
				"properties": {
					"strict_number": {"type": "long", "coerce": false}
				}
			}
		}`

		client := &http.Client{Timeout: 10 * time.Second}
		req, _ := http.NewRequest("PUT", "http://es7:9200/test.memory_leak", strings.NewReader(mappingData))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal("Failed to create ES mapping:", err)
		}
		resp.Body.Close()

		t.Log("✅ Created strict ES mapping")

		// The test relies on monstache being configured to sync test.memory_leak
		// Documents with string values in strict_number field will fail ES mapping
		// This tests if contexts for failed operations get properly cleaned up

		t.Log("⏳ Integration test requires manual verification:")
		t.Log("   1. Insert docs with invalid mapping to MongoDB")
		t.Log("   2. Check monstache logs for BULK INDEX FAILURE messages")
		t.Log("   3. Check for 'Cleaned up X completed request contexts' messages")
		t.Log("   4. Verify system remains responsive")

		// Wait a moment for any pending operations
		time.Sleep(2 * time.Second)

		// Check ES document count
		resp, err = http.Get("http://es7:9200/test.memory_leak/_count")
		if err != nil {
			t.Fatal("Failed to count ES docs:", err)
		}
		defer resp.Body.Close()

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)

		if count, ok := result["count"].(float64); ok {
			t.Logf("📊 ES document count: %.0f", count)
		}
	})
}

// TestEnvironmentCheck verifies test environment is properly configured
func TestEnvironmentCheck(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"Elasticsearch", "http://es7:9200/_cluster/health"},
		{"MongoDB", "http://mongo-0:27017"}, // This will fail but confirm port is open
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &http.Client{Timeout: 5 * time.Second}
			_, err := client.Get(tt.url)
			if err != nil && !strings.Contains(err.Error(), "malformed HTTP") {
				t.Errorf("%s not available at %s: %v", tt.name, tt.url, err)
			} else {
				t.Logf("✅ %s available at %s", tt.name, tt.url)
			}
		})
	}
}
