// Package memory is an in-memory implementation for the platform.
// This implementation is intended entirely for development purposes and demoability.
// This implementation will not and cannot scale beyond a single node and offers zero persistence.
package memory

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"bytes"

	"github.com/oceanhq/streams/platform"
)

const (
	ID_LENGTH   = 16 // 128-bit
	MAX_RECORDS = 100
)

type InMemoryPlatform struct {
	streams []stream
	cursors []cursor
}

func (p *InMemoryPlatform) CreateStream(name string) (*platform.Stream, error) {
	if name == "" {
		return nil, platform.InvalidParamError(errors.New("Invalid param: `name` must not be empty"))
	}

	id, err := generateId()
	if err != nil {
		return nil, err
	}

	stream := stream{
		id:   id,
		name: name}

	p.streams = append(p.streams, stream)

	return (&stream).toExt(), nil
}

func (p *InMemoryPlatform) ListStreams() ([]platform.Stream, error) {
	out := make([]platform.Stream, len(p.streams))
	for i := 0; i < len(p.streams); i++ {
		s := &(p.streams[i])
		out[i] = *(s.toExt())
	}

	return out, nil
}

func (p *InMemoryPlatform) CreateCursor(streamId string) (*platform.Cursor, error) {
	stream, err := p.findStream(streamId)
	if err != nil {
		return nil, platform.InvalidParamError(fmt.Errorf("\"%s\" is not a valid stream ID.", streamId))
	} else if stream == nil {
		return nil, platform.StreamNotFoundError(fmt.Errorf("The stream \"%s\" does not exist.", streamId))
	}

	id, err := generateId()
	if err != nil {
		return nil, err
	}

	cursor := cursor{
		id:       id,
		stream:   stream,
		position: stream.findLastRecord()}

	p.cursors = append(p.cursors, cursor)

	return (&cursor).toExt(), nil
}

func (p *InMemoryPlatform) CreateRecord(streamId string, content []byte) (*platform.Record, error) {
	stream, err := p.findStream(streamId)
	if err != nil {
		return nil, platform.InvalidParamError(fmt.Errorf("\"%s\" is not a valid stream ID. %s", streamId, err.Error()))
	} else if stream == nil {
		return nil, platform.StreamNotFoundError(fmt.Errorf("The stream \"%s\" does not exist.", streamId))
	}

	record := &record{
		id:        nextRecordId(),
		stream:    stream,
		content:   content,
		timestamp: time.Now().UTC()}

	previousRec := stream.findLastRecord()
	if previousRec == nil {
		stream.root = record
	} else {
		previousRec.next = record
	}

	return record.toExt(), nil
}

func (p *InMemoryPlatform) GetRecords(streamId string, cursorId string) ([]platform.Record, error) {
	stream, err := p.findStream(streamId)
	if err != nil {
		return nil, platform.InvalidParamError(fmt.Errorf("\"%s\" is not a valid stream ID. %s", streamId, err.Error()))
	} else if stream == nil {
		return nil, platform.StreamNotFoundError(fmt.Errorf("The stream \"%s\" does not exist.", streamId))
	}

	cursor, err := p.findCursor(cursorId)
	if err != nil {
		return nil, platform.InvalidParamError(fmt.Errorf("\"%s\" is not a valid cursor ID. %s", cursorId, err.Error()))
	} else if cursor == nil {
		return nil, platform.CursorNotFoundError(fmt.Errorf("The cursor \"%s\" does not exist.", cursorId))
	}

	res := []platform.Record{}

	// Return the empty array if there are no records in the stream or if already positioned on the last record.
	if stream.root == nil || (cursor.position != nil && cursor.position.next == nil) {
		return res, nil
	}

	// Either initialise the cursor to the root record or step the cursor ahead
	if cursor.position != nil {
		cursor.position = cursor.position.next
	} else {
		cursor.position = stream.root
	}

	// Otherwise, start reading through the records
	for len(res) < MAX_RECORDS {
		rec := cursor.position.toExt()

		res = append(res, *rec)

		if cursor.position.next == nil {
			break
		} else {
			cursor.position = cursor.position.next
		}
	}

	return res, nil
}

func generateId() ([]byte, error) {
	b := make([]byte, ID_LENGTH)
	_, err := rand.Read(b)

	return b, err
}

func nextRecordId() []byte {
	lastRecordId.Add(lastRecordId, big.NewInt(1))
	return lastRecordId.Bytes()
}

func (p *InMemoryPlatform) findStream(streamId string) (*stream, error) {
	// Parse ID
	byteId, err := hex.DecodeString(streamId)
	if err != nil {
		return nil, err
	}

	list := p.streams

	for i := 0; i < len(list); i++ {
		if bytes.Equal(list[i].id, byteId) {
			return &(list[i]), nil
		}
	}

	return nil, nil
}

func (p *InMemoryPlatform) findCursor(cursorId string) (*cursor, error) {
	// Parse ID
	byteId, err := hex.DecodeString(cursorId)
	if err != nil {
		return nil, err
	}

	list := p.cursors

	for i := 0; i < len(list); i++ {
		if bytes.Equal(list[i].id, byteId) {
			return &(list[i]), nil
		}
	}

	return nil, nil
}

type stream struct {
	id   []byte
	name string
	root *record
}

type cursor struct {
	id       []byte
	stream   *stream
	position *record
}

type record struct {
	id        []byte
	stream    *stream
	content   []byte
	timestamp time.Time
	next      *record
}

func (s *stream) toExt() *platform.Stream {
	return &platform.Stream{
		Id:   hex.EncodeToString(s.id),
		Name: s.name}
}

func (c *cursor) toExt() *platform.Cursor {
	ext := &platform.Cursor{
		Id: hex.EncodeToString(c.id)}

	if c.stream != nil {
		ext.StreamId = hex.EncodeToString(c.stream.id)
	}

	if c.position != nil {
		ext.Position = c.position.idToString()
	} else {
		ext.Position = "-1"
	}

	return ext
}

func (s *stream) findLastRecord() *record {
	var lastRecord *record = s.root
	if lastRecord == nil {
		return nil
	}

	for ; lastRecord.next != nil; lastRecord = lastRecord.next {
	}

	return lastRecord
}

func (r *record) toExt() *platform.Record {
	h := sha1.New()
	h.Write(r.content)

	return &platform.Record{
		Id:          r.idToString(),
		StreamId:    hex.EncodeToString(r.stream.id),
		Content:     r.content,
		ContentHash: h.Sum(nil),
		Timestamp:   r.timestamp}
}

func (r *record) idToString() string {
	recId := &big.Int{}
	recId.SetBytes(r.id)
	return recId.String()
}

var (
	lastRecordId = big.NewInt(0)
)
