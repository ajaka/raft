package db

import (
	"encoding/binary"
	"raft/internal/log"
	rpc "raft/internal/rpc/gen"
)

type KVStore struct {
	db map[string][]byte
}

func InitDB() *KVStore {
	return &KVStore{
		db: make(map[string][]byte),
	}
}

func (k *KVStore) set(key []byte, value []byte) {
	k.db[string(key)] = value
}

func (k *KVStore) get(key []byte) ([]byte, bool) {
	v, ok := k.db[string(key)]
	return v, ok
}

func (k *KVStore) del(key []byte) {
	delete(k.db, string(key))
}

func (k *KVStore) Apply(e log.Entry) {
	switch e.Type {
	case rpc.EntryType_SET:
		k.handleSet(e.Payload)
	case rpc.EntryType_DEL:
		k.handleDel(e.Payload)
	}
}

func (k *KVStore) handleDel(payload []byte) {
	keyLengthByte := payload[0:4]
	keyLength := binary.BigEndian.Uint32(keyLengthByte)
	keyByte := payload[8:keyLength]
	k.del(keyByte)
}

func (k *KVStore) handleSet(payload []byte) {

	keyLengthByte := payload[0:4]
	valueLengthByte := payload[4:8]
	keyLength := binary.BigEndian.Uint32(keyLengthByte)
	valueLength := binary.BigEndian.Uint32(valueLengthByte)
	keyByte := payload[8:keyLength]
	valueByte := payload[keyLength:valueLength]
	k.set(keyByte, valueByte)
}
