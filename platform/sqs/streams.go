package sqs

import (
	"fmt"
	"log"

	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/oceanhq/streams/platform"
)

func (p *SqsPlatform) CreateStream(name string) (*platform.Stream, error) {
	streamId, err := generateId()
	if err != nil {
		return nil, err
	}

	arn, err := createStreamSNSTopic(streamId)
	if err != nil {
		return nil, err
	}

	err = createStreamDBItem(streamId, name, arn)
	if err != nil {
		return nil, err
	}

	res := &platform.Stream{
		Id:   streamId,
		Name: name}

	return res, nil
}

func createStreamDBItem(streamId string, name string, topicArn string) error {
	tableName := TABLE_STREAMS

	attrs := map[string]*dynamodb.AttributeValue{
		COLUMN_STREAM_ID:          &dynamodb.AttributeValue{S: &streamId},
		COLUMN_STREAM_NAME:        &dynamodb.AttributeValue{S: &name},
		COLUMN_STREAM_SNSTOPICARN: &dynamodb.AttributeValue{S: &topicArn}}

	_, err := svcDynamoDb.PutItem(&dynamodb.PutItemInput{
		TableName: &tableName,
		Item:      attrs})

	return err
}

func createStreamSNSTopic(streamId string) (string, error) {
	topicName := fmt.Sprintf("%s%s", SNS_TOPIC_PREFIX, streamId)
	out, err := svcSns.CreateTopic(&sns.CreateTopicInput{
		Name: &topicName})
	if err != nil {
		return "", err
	}

	return *out.TopicArn, nil
}

func (p *SqsPlatform) ListStreams() ([]platform.Stream, error) {
	tableName := TABLE_STREAMS

	// "Name" is a reserved keyword in DynamoDB so we need to use an ExpressionAttributeName to request it.
	name := COLUMN_STREAM_NAME
	ean := map[string]*string{
		"#n": &name}

	attrs := strings.Join([]string{COLUMN_STREAM_ID, "#n"}, ",")

	out, err := svcDynamoDb.Scan(&dynamodb.ScanInput{
		TableName:                &tableName,
		ProjectionExpression:     &attrs,
		ExpressionAttributeNames: ean})
	if err != nil {
		return nil, err
	}

	streams := make([]platform.Stream, *out.Count)
	for idx, item := range out.Items {
		stream := platform.Stream{
			Id:   *(item[COLUMN_STREAM_ID].S),
			Name: *(item[COLUMN_STREAM_NAME].S)}

		streams[idx] = stream
	}

	return streams, nil
}

func (p *SqsPlatform) GetStream(streamId string) (*platform.Stream, error) {
	err := validateId(streamId)
	if err != nil {
		// This is an expected error so don't treat as fatal.
		log.Printf("An invalid stream ID was requested: %s", err)

		err = &platform.ErrInvalidParam{
			Param: "StreamID",
			Value: streamId,
			Err:   err}

		return nil, err
	}

	tableName := TABLE_STREAMS

	// "Name" is a reserved keyword in DynamoDB so we need to use an ExpressionAttributeName to request it.
	name := COLUMN_STREAM_NAME
	ean := map[string]*string{
		"#n": &name}

	attrs := fmt.Sprintf("%s,#n", COLUMN_STREAM_ID)

	key := map[string]*dynamodb.AttributeValue{
		COLUMN_STREAM_ID: &dynamodb.AttributeValue{S: &streamId}}
	out, err := svcDynamoDb.GetItem(&dynamodb.GetItemInput{
		TableName:                &tableName,
		ExpressionAttributeNames: ean,
		ProjectionExpression:     &attrs,
		Key:                      key})
	if err != nil {
		return nil, err
	}

	if len(out.Item) == 0 {
		return nil, &platform.ErrStreamNotFound{
			SearchParam: "ID",
			Value:       streamId}
	}

	res := &platform.Stream{
		Id:   *(out.Item[COLUMN_STREAM_ID].S),
		Name: *(out.Item[COLUMN_STREAM_NAME].S)}

	return res, nil
}

func getStreamTopicArn(streamId string) (string, error) {
	tableName := TABLE_STREAMS
	attrs := strings.Join([]string{COLUMN_STREAM_SNSTOPICARN}, ",")

	key := map[string]*dynamodb.AttributeValue{
		COLUMN_STREAM_ID: &dynamodb.AttributeValue{S: &streamId}}
	out, err := svcDynamoDb.GetItem(&dynamodb.GetItemInput{
		TableName:            &tableName,
		ProjectionExpression: &attrs,
		Key:                  key})
	if err != nil {
		return "", err
	}

	if len(out.Item) == 0 {
		return "", &platform.ErrStreamNotFound{
			SearchParam: "ID",
			Value:       streamId}
	}

	return *out.Item[COLUMN_STREAM_SNSTOPICARN].S, nil
}
