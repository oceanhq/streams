package sqs

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const (
	ID_LENGTH          = 16 // 128-bit
	DEFAULT_CURSOR_POS = "-1"

	TABLE_STREAMS = "ocean-streams"
	TABLE_CURSORS = "ocean-cursors"

	COLUMN_STREAM_ID          = "StreamId"
	COLUMN_STREAM_NAME        = "Name"
	COLUMN_STREAM_SNSTOPICARN = "SNSTopicARN"
	COLUMN_CURSOR_ID          = "CursorId"
	COLUMN_CURSOR_POSITION    = "Position"
	COLUMN_CURSOR_SQSQUEUEURL = "SQSQueueURL"

	SNS_TOPIC_PREFIX = "ocean_stream-"
	SQS_QUEUE_PREFIX = "ocean_cursor-"
)

var (
	sess        = session.New()
	svcDynamoDb = dynamodb.New(sess)
	svcSns      = sns.New(sess)
	svcSqs      = sqs.New(sess)
)

type SqsPlatform struct {
}

func generateId() (string, error) {
	b := make([]byte, ID_LENGTH)
	_, err := rand.Read(b)

	return hex.EncodeToString(b), err
}

func validateId(id string) error {
	if id == "" {
		return errors.New("ID must not be empty")
	}

	bytes, err := hex.DecodeString(id)
	if err != nil {
		return err
	}

	if len(bytes) != ID_LENGTH {
		return fmt.Errorf("IDs must be %d bytes", ID_LENGTH)
	}

	return nil
}
