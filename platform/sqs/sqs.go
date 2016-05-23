package sqs

import (
	"errors"

	"github.com/oceanhq/streams/platform"
)

type SqsPlatform struct {
}

func (p *SqsPlatform) CreateStream(name string) (*platform.Stream, error) {
	return nil, errors.New("Function not implemented")
}
func (p *SqsPlatform) ListStreams() ([]platform.Stream, error) {
	return nil, errors.New("Function not implemented")
}
func (p *SqsPlatform) GetStream(streamId string) (*platform.Stream, error) {
	return nil, errors.New("Function not implemented")
}
func (p *SqsPlatform) CreateCursor(streamId string) (*platform.Cursor, error) {
	return nil, errors.New("Function not implemented")
}
func (p *SqsPlatform) CreateRecord(streamId string, content []byte) (*platform.Record, error) {
	return nil, errors.New("Function not implemented")
}
func (p *SqsPlatform) GetRecords(streamId string, cursorId string) ([]platform.Record, error) {
	return nil, errors.New("Function not implemented")
}
