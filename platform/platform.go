package platform

import "time"

type Platform interface {
	CreateStream(name string) (*Stream, error)
	ListStreams() ([]Stream, error)
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

type InvalidParamError error

type StreamNotFoundError error

type CursorNotFoundError error
