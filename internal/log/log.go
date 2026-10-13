package log

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
)

type Log struct {
	index        map[uint64]uint64
	filename     string
	file         *os.File
	lastLogIndex uint64
	lastLogTerm  uint64
}

func InitLog() (*Log, error) {
	log := Log{
		index:        make(map[uint64]uint64),
		filename:     "raft.log",
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
	err := log.Init()
	if err != nil {
		return nil, err
	}
	return &log, nil
}

func (l *Log) GetLastIndex() uint64 {
	return l.lastLogIndex
}

func (l *Log) GetLastTerm() uint64 {
	return l.lastLogTerm
}

func (l *Log) TermAt(index uint64) (uint64, bool) {
	offset, ok := l.index[index]
	if !ok {
		return 0, ok
	}
	buf, ok := l.getExact(offset)
	if !ok {
		return 0, ok
	}
	entry, err := l.readEntry(buf)
	if err != nil {
		return 0, false
	}
	return entry.Term, true
}

func (l *Log) truncate(index uint64) {
	if index == 0 {
		l.file.Truncate(0)
		l.BuildIndex()
		return
	}
	offset, ok := l.index[index]
	if !ok {
		return
	}
	l.file.Truncate(int64(offset))
	l.BuildIndex()
}

func (l *Log) getExact(offset uint64) ([]byte, bool) {
	l.file.Seek(int64(offset), io.SeekStart)
	buf := make([]byte, 4)
	n, err := io.ReadFull(l.file, buf)
	if err != nil || n != 4 {
		l.file.Truncate(int64(offset))
		return nil, false
	}
	length := int(binary.BigEndian.Uint32(buf))
	bodyBuf := make([]byte, length)
	nn, err := io.ReadFull(l.file, bodyBuf)
	if err != nil || nn != length {
		l.file.Truncate(int64(offset))
	}
	return bodyBuf, true
}

func (l *Log) GetEntry(index uint64) (Entry, bool) {
	offset, ok := l.index[index]
	if !ok {
		return Entry{}, ok
	}
	buf, ok := l.getExact(offset)
	if !ok {
		return Entry{}, ok
	}
	entry, err := l.readEntry(buf)
	if err != nil {
		return Entry{}, ok
	}
	return entry, true
}

func (l *Log) GetEntries(index uint64) []Entry {
	var entries []Entry
	for index <= l.lastLogIndex {
		entry, ok := l.GetEntry(index)
		if !ok {
			break
		}
		entries = append(entries, entry)
		index++
	}
	return entries
}
func (l *Log) Init() error {
	file, err := os.OpenFile(l.filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	l.file = file
	l.BuildIndex()
	return nil
}

func (l *Log) BuildIndex() {
	for k := range l.index {
		delete(l.index, k)
	}
	buf := make([]byte, 4)
	var bodyBuf []byte
	offset := 0
	index := 1
	l.index[uint64(index)] = 0
	any := false
	for {
		index++
		l.file.Seek(int64(offset), io.SeekStart)
		n, err := io.ReadFull(l.file, buf)
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				l.file.Truncate(int64(offset))
			}
			break
		}
		offset += n
		length := int(binary.BigEndian.Uint32(buf))
		bodyBuf = make([]byte, length)
		nn, err := io.ReadFull(l.file, bodyBuf)
		if err != nil {
			l.file.Truncate(int64(offset - n))
			break
		}
		offset += nn
		l.index[uint64(index)] = uint64(offset)
		any = true
	}
	if !any {
		l.lastLogIndex = 0
		l.lastLogTerm = 0
	}
	e, _ := l.readEntry(bodyBuf)
	l.lastLogIndex = e.Index
	l.lastLogTerm = e.Term
}

func (l *Log) readEntry(buf []byte) (Entry, error) {
	var entry Entry
	err := json.Unmarshal(buf, &entry)
	return entry, err
}

func (l *Log) AppendEntries(entries []Entry) error {
	conflictIndex := uint64(0)
	for _, e := range entries {
		existingTerm, ok := l.TermAt(e.Index)
		if !ok {
			conflictIndex = e.Index
			break
		}
		if existingTerm != e.Term {
			conflictIndex = e.Index
			break
		}
	}
	if conflictIndex > 0 {
		l.truncate(conflictIndex)
	}

	for _, e := range entries {
		ebytes, err := json.Marshal(e)
		if err != nil {
			return err
		}
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(ebytes)))
		_, err = l.file.Write(lenBuf)
		if err != nil {
			return err
		}
		_, err = l.file.Write(ebytes)
		if err != nil {
			return err
		}
		l.lastLogIndex = e.Index
		l.lastLogTerm = e.Term
	}
	err := l.file.Sync()
	if err != nil {
		return err
	}
	l.BuildIndex()
	return nil
}
