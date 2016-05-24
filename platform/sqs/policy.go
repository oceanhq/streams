package sqs

import (
	"bytes"
	"html/template"

	"fmt"
	"os"
)

const (
	SQS_POLICY_TEMPLATE = `{
  "Version": "2012-10-17",
  "Id": "{{.PredictSQSQueueARN}}/SQSDefaultPolicy",
  "Statement": [
    {
      "Sid": "Sid1464041142853",
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "SQS:SendMessage",
      "Resource": "{{.PredictSQSQueueARN}}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "{{.FetchSNSTopicARN}}"
        }
      }
    }
  ]
}`
	SQS_QUEUE_ARN_FORMAT = `arn:aws:sqs:us-west-2:%s:%s`
)

type sqsPolicy struct {
	streamId string
	cursorId string
	topicArn string
}

func (p *sqsPolicy) PredictSQSQueueARN() string {
	return getQueueArn(p.cursorId)
}

func (p *sqsPolicy) FetchSNSTopicARN() (string, error) {
	err := validateId(p.streamId)
	if err != nil {
		return "", err
	}

	if p.topicArn != "" {
		return p.topicArn, nil
	}

	topicArn, err := getStreamTopicArn(p.streamId)
	if err != nil {
		return "", err
	}

	p.topicArn = topicArn

	return p.topicArn, nil
}

func (p *sqsPolicy) buildPolicy() (string, error) {
	tmpl, err := template.New("SQSPolicy").Parse(SQS_POLICY_TEMPLATE)
	if err != nil {
		return "", err
	}

	buff := &bytes.Buffer{}

	err = tmpl.Execute(buff, p)
	if err != nil {
		return "", err
	}

	policy := buff.String()

	return policy, nil
}

func getQueueArn(cursorId string) string {
	acctNum := os.Getenv("AWS_ACCOUNT_ID")
	return fmt.Sprintf(SQS_QUEUE_ARN_FORMAT, acctNum, fmt.Sprintf("%s%s", SQS_QUEUE_PREFIX, cursorId))
}
