package sqs

import (
	"fmt"

	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/oceanhq/streams/platform"
)

func (p *SqsPlatform) CreateCursor(streamId string) (*platform.Cursor, error) {
	cursorId, err := generateId()
	if err != nil {
		return nil, err
	}

	cursorPosition := DEFAULT_CURSOR_POS

	url, err := createCursorSQSQueue(cursorId, streamId)
	if err != nil {
		return nil, err
	}

	// Subscribe SQS Queue to SNS Topic
	protocol := "sqs"
	endpoint := getQueueArn(cursorId)
	topicArn, err := getStreamTopicArn(streamId)
	if err != nil {
		return nil, err
	}

	svcSns.Subscribe(&sns.SubscribeInput{
		Protocol: &protocol,
		Endpoint: &endpoint,
		TopicArn: &topicArn,
	})

	err = createCursorDBItem(cursorId, streamId, cursorPosition, url)
	if err != nil {
		return nil, err
	}

	res := &platform.Cursor{
		Id:       cursorId,
		StreamId: streamId,
		Position: cursorPosition}

	return res, nil
}

func createCursorDBItem(cursorId string, streamId string, cursorPosition string, queueUrl string) error {
	tableName := TABLE_CURSORS

	attrs := map[string]*dynamodb.AttributeValue{
		COLUMN_CURSOR_ID:          &dynamodb.AttributeValue{S: &cursorId},
		COLUMN_STREAM_ID:          &dynamodb.AttributeValue{S: &streamId},
		COLUMN_CURSOR_POSITION:    &dynamodb.AttributeValue{N: &cursorPosition},
		COLUMN_CURSOR_SQSQUEUEURL: &dynamodb.AttributeValue{S: &queueUrl}}

	_, err := svcDynamoDb.PutItem(&dynamodb.PutItemInput{
		TableName: &tableName,
		Item:      attrs})

	return err
}

func createCursorSQSQueue(cursorId string, streamId string) (string, error) {
	policyTmpl := &sqsPolicy{
		cursorId: cursorId,
		streamId: streamId}

	queueName := fmt.Sprintf("%s%s", SQS_QUEUE_PREFIX, cursorId)
	println("Generating policy...")
	policy, err := policyTmpl.buildPolicy()
	if err != nil {
		return "", err
	}

	attrs := map[string]*string{
		"Policy": &policy}

	out, err := svcSqs.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  &queueName,
		Attributes: attrs})
	if err != nil {
		return "", err
	}

	return *out.QueueUrl, nil
}

func getCursorQueueUrl(streamId string, cursorId string) (string, error) {
	tableName := TABLE_CURSORS

	attrs := strings.Join([]string{COLUMN_CURSOR_SQSQUEUEURL}, ",")

	key := map[string]*dynamodb.AttributeValue{
		COLUMN_STREAM_ID: &dynamodb.AttributeValue{S: &streamId},
		COLUMN_CURSOR_ID: &dynamodb.AttributeValue{S: &cursorId}}
	out, err := svcDynamoDb.GetItem(&dynamodb.GetItemInput{
		TableName:            &tableName,
		ProjectionExpression: &attrs,
		Key:                  key})
	if err != nil {
		return "", err
	}

	if len(out.Item) == 0 {
		return "", &platform.ErrCursorNotFound{CursorID: cursorId, StreamID: streamId}
	}

	return *out.Item[COLUMN_CURSOR_SQSQUEUEURL].S, nil
}
