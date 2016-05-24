package platform

import (
	"fmt"
	"time"
)

type Platform interface {
	CreateStream(name string) (*Stream, error)
	ListStreams() ([]Stream, error)
	GetStream(streamId string) (*Stream, error)
	CreateCursor(streamId string) (*Cursor, error)
	CreateRecord(streamId string, content []byte) (*Record, error)
	GetRecords(streamId string, cursorId string) ([]Record, error)
}

type Stream struct {
	Id   string
	Name string
}

type Cursor struct {
	Id       string
	StreamId string
	Position string
}

type Record struct {
	Id          string
	StreamId    string
	Content     []byte
	ContentHash []byte
	Timestamp   time.Time
}

type ErrInvalidParam struct {
	Param string
	Value string
	Err   error
}

func (e *ErrInvalidParam) Error() string {
	return fmt.Sprintf("\"%s\" is not a valid value for param \"%s\". %s", e.Value, e.Param, e.Err.Error())
}

type ErrStreamNotFound struct {
	SearchParam string
	Value       string
}

func (e *ErrStreamNotFound) Error() string {
	return fmt.Sprintf("A stream with the %s \"%s\" does not exist.", e.SearchParam, e.Value)
}

type ErrCursorNotFound struct {
	CursorID string
	StreamID string
}

func (e *ErrCursorNotFound) Error() string {
	return fmt.Sprintf("A cursor with ID \"%s\" does not exist for a stream with ID \"%s\".", e.CursorID, e.StreamID)
}
