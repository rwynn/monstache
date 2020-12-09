package oplog

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"sync"
	"time"
)

// A TimestampResolver decides on a timestamp from which to start reading an oplog from.
// A result may not be immediately available (see TimestampResolverEarliest), so it is returned in a channel.
type TimestampResolver interface {
	GetResumeTimestamp(candidateTs primitive.Timestamp) chan primitive.Timestamp
}

// A simple resolver immediately returns a timestamp it's been given.
type TimestampResolverPolicySimple struct{}

func (r TimestampResolverPolicySimple) GetResumeTimestamp(candidateTs primitive.Timestamp) chan primitive.Timestamp {
	tmpResultChan := make(chan primitive.Timestamp, 1)
	tmpResultChan <- candidateTs

	return tmpResultChan
}

// TimestampResolverEarliest waits until oplog resume timestamps have been queried from all the available mongodb shards, and returns an earliest one.
type TimestampResolverEarliest struct {
	connectionsTotal   int
	connectionsQueried int
	currentTs          primitive.Timestamp
	resultChan         chan primitive.Timestamp
	logger             *log.Logger
	m                  sync.Mutex
}

func NewTimestampResolverEarliest(connectionsTotal int, logger *log.Logger) *TimestampResolverEarliest {
	return &TimestampResolverEarliest{
		connectionsTotal: connectionsTotal,
		resultChan:       make(chan primitive.Timestamp, connectionsTotal),
		logger:           logger,
	}
}

// Returns a channel from which an earliest resume timestamp can be received
func (oplogResume *TimestampResolverEarliest) GetResumeTimestamp(candidateTs primitive.Timestamp) chan primitive.Timestamp {
	oplogResume.m.Lock()
	defer oplogResume.m.Unlock()

	if oplogResume.connectionsQueried >= oplogResume.connectionsTotal {
		// in this case, an earliest timestamp is already calculated,
		// so it is just returned in a temporary channel
		oplogResume.logger.Printf(
			"Earliest oplog resume timestamp is already calculated: %s",
			(time.Unix(int64(oplogResume.currentTs.T), 0)).Format(time.RFC3339),
		)

		tmpResultChan := make(chan primitive.Timestamp, 1)
		tmpResultChan <- oplogResume.currentTs

		return tmpResultChan
	}

	oplogResume.connectionsQueried++

	// if a candidate timestamp is smaller than a currently known timestamp,
	// then a current timestamp is updated
	if oplogResume.currentTs.T == 0 || primitive.CompareTimestamp(candidateTs, oplogResume.currentTs) < 0 {
		oplogResume.currentTs = candidateTs
		oplogResume.logger.Printf(
			"Oplog resume timestamp is updated: %s",
			tsToTime(oplogResume.currentTs).Format(time.RFC3339),
		)
	}

	// if this function has been called for every mongodb connection,
	// then a final earliest resume timestamp can be returned to every caller
	if oplogResume.connectionsQueried == oplogResume.connectionsTotal {
		oplogResume.logger.Printf(
			"Earliest oplog resume timestamp calculated: %s",
			tsToTime(oplogResume.currentTs).Format(time.RFC3339),
		)

		for i := 0; i < oplogResume.connectionsTotal; i++ {
			oplogResume.resultChan <- oplogResume.currentTs
		}
	}

	return oplogResume.resultChan
}

// Converts a bson timestamp to go time.Time
func tsToTime(ts primitive.Timestamp) time.Time {
	return time.Unix(int64(ts.T), int64(ts.I))
}
