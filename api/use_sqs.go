// +build sqs heroku

package api

import (
	"github.com/oceanhq/streams/platform"
	"github.com/oceanhq/streams/platform/sqs"
)

var platformImpl platform.Platform = &sqs.SqsPlatform{}
