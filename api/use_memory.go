// +build !sqs

package api

import (
	"github.com/oceanhq/streams/platform"
	"github.com/oceanhq/streams/platform/memory"
)

var platformImpl platform.Platform = &memory.InMemoryPlatform{}
