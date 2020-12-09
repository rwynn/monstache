package oplog

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"os"
	"testing"
)

// Tests an earliest timestamp resolver with 3 mongodb shards
func TestTimestampResolverEarliest_GetResumeTimestamp_ThreeShards(t *testing.T) {
	resolver := NewTimestampResolverEarliest(3, log.New(os.Stdout, "INFO ", log.Flags()))
	timestampA := primitive.Timestamp{
		T: 1000,
		I: 10,
	}
	timestampB := primitive.Timestamp{
		T: 1000,
		I: 5,
	}
	timestampC := primitive.Timestamp{
		T: 100500,
		I: 100500,
	}

	chanA := resolver.GetResumeTimestamp(timestampA)
	chanB := resolver.GetResumeTimestamp(timestampB)
	chanC := resolver.GetResumeTimestamp(timestampC)

	resultA := <-chanA
	resultB := <-chanB
	resultC := <-chanC

	if resultA.T != 1000 || resultA.I != 5 {
		t.Fatalf(
			"Expected an earliest timestamp to be 1000.5, got %d.%d",
			resultA.T,
			resultA.I,
		)
	}

	if !resultB.Equal(resultA) || !resultC.Equal(resultA) {
		t.Fatalf(
			"An earliest timestamp must be consistent for all callers",
		)
	}

	repeatedCallResult := <-resolver.GetResumeTimestamp(primitive.Timestamp{
		T: 1,
		I: 1,
	})
	if !repeatedCallResult.Equal(resultA) {
		t.Fatalf(
			"A repeated call to GetResumeTimestamp must return a previously calculated timestamp; got %d.%d.",
			repeatedCallResult.T,
			repeatedCallResult.I,
		)
	}
}

// Tests an earliest timestamp resolver with a single mongodb shard
func TestTimestampResolverEarliest_GetResumeTimestamp_SingleShard(t *testing.T) {
	resolver := NewTimestampResolverEarliest(1, log.New(os.Stdout, "INFO ", log.Flags()))

	result := <-resolver.GetResumeTimestamp(primitive.Timestamp{
		T: 1000,
		I: 3,
	})

	if result.T != 1000 || result.I != 3 {
		t.Fatalf(
			"Expected an earliest timestamp to be 1000.3, got %d.%d",
			result.T,
			result.I,
		)
	}
}
