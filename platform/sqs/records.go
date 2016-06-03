package sqs

import (
	"errors"

	"encoding/base64"

	"crypto/sha1"
	"encoding/hex"
	"time"

	"encoding/json"

	"bytes"

	"log"

	"fmt"

	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/oceanhq/streams/platform"
)

const (
	TIME_FORMAT = time.RFC3339Nano
)

var (
	base64Encoding = base64.StdEncoding
)

func (p *SqsPlatform) CreateRecord(streamId string, content []byte) (*platform.Record, error) {
	err := validateId(streamId)
	if err != nil {
		return nil, err
	}

	topicArn, err := getStreamTopicArn(streamId)
	if err != nil {
		return nil, err
	}

	recordId, err := generateId()
	if err != nil {
		return nil, err
	}

	encContent := base64Encoding.EncodeToString(content)

	hash := hashContent(content)

	timestamp := time.Now().UTC()

	record := &record{
		RecordId:    recordId,
		StreamId:    streamId,
		Content:     encContent,
		ContentHash: hex.EncodeToString(hash),
		Timestamp:   timestamp.Format(TIME_FORMAT)}

	bJson, err := json.Marshal(record)

	// Base64 encode the content
	enc := base64Encoding.EncodeToString(bJson)

	out, err := svcSns.Publish(&sns.PublishInput{
		TopicArn: &topicArn,
		Message:  &enc})
	if err != nil {
		return nil, err
	}

	if *out.MessageId == "" {
		return nil, errors.New("Failed to send message.")
	}

	res := &platform.Record{
		Id:          recordId,
		StreamId:    streamId,
		Content:     content,
		ContentHash: hash,
		Timestamp:   timestamp}

	return res, nil
}

func (p *SqsPlatform) GetRecords(streamId string, cursorId string) ([]platform.Record, error) {
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

	err = validateId(cursorId)
	if err != nil {
		// This is an expected error so don't treat as fatal.
		log.Printf("An invalid cursor ID was requested: %s", err)

		err = &platform.ErrInvalidParam{
			Param: "CursorID",
			Value: streamId,
			Err:   err}

		return nil, err
	}

	var maxNumberOfMessages int64 = 10
	queueUrl, err := getCursorQueueUrl(streamId, cursorId)
	if err != nil {
		// This is an expected error (in the event the cursor does not exist) so don't treat as fatal.
		log.Printf("Error getting cursor Queue URL: %s", err)
		return nil, err
	}

	out, err := svcSqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: &maxNumberOfMessages,
		QueueUrl:            &queueUrl})
	if err != nil {
		log.Fatalf("Error receiving message from SQS service: %s", err)
		return nil, err
	}

	results := make([]platform.Record, len(out.Messages))
	for k, sqsMessage := range out.Messages {
		bodyJson := *sqsMessage.Body

		body := &sqsMessageBody{}
		err = json.Unmarshal([]byte(bodyJson), body)
		if err != nil {
			log.Fatalf("Error parsing message body json: %s", err)

			// This will mean we return none of the records on error which may not be ideal
			return nil, err
		}

		// Deserialize internal message format
		bJson, err := base64Encoding.DecodeString(body.Message)
		if err != nil {
			log.Fatalf("Error decoding message body: %s (body: %s)", err, body)

			// This will mean we return none of the records on error which may not be ideal
			return nil, err
		}

		rec := &record{}
		err = json.Unmarshal(bJson, rec)
		if err != nil {
			log.Fatalf("Error parsing message json: %s", err)

			// This will mean we return none of the records on error which may not be ideal
			return nil, err
		}

		// Compute a fresh hash
		bContent, err := base64Encoding.DecodeString(rec.Content)
		if err != nil {
			log.Fatalf("Error decoding record content: %s", err)

			// This will mean we return none of the records on error which may not be ideal
			return nil, err
		}
		hash := hashContent(bContent)

		// Compare the hashes to ensure the data integrity
		originalHash, err := hex.DecodeString(rec.ContentHash)
		if err != nil {
			log.Fatalf("Error decoding original record content hash: %s", err)

			return nil, err
		}
		if !bytes.Equal(hash, originalHash) {
			err = errors.New(fmt.Sprintf("The content hash did not match the content. Expected: %X; Received: %X", originalHash, hash))
			log.Fatalf("Error validating content hash: %s", err)

			return nil, err
		}

		timestamp, err := time.Parse(TIME_FORMAT, rec.Timestamp)
		if err != nil {
			log.Fatalf("Error parsing record timestamp: %s", err)
			return nil, err
		}

		results[k] = platform.Record{
			Id:          rec.RecordId,
			StreamId:    rec.StreamId,
			Content:     bContent,
			ContentHash: hash,
			Timestamp:   timestamp}
	}

	return results, nil
}

func hashContent(content []byte) []byte {
	h := sha1.New()
	h.Write(content)
	return h.Sum(nil)
}

type record struct {
	RecordId    string `json:"recordId"`
	StreamId    string `json:"streamId"`
	Content     string `json:"content"`
	ContentHash string `json:"contentHash"`
	Timestamp   string `json:"timestamp"`
}

type sqsMessageBody struct {
	Message string
}
