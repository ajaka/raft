package log

import rpc "raft/internal/rpc/gen"

type Entry struct {
	Index   uint64        `json:"index,omitempty"`
	Term    uint64        `json:"term,omitempty"`
	Type    rpc.EntryType `json:"type,omitempty"`
	Payload []byte        `json:"payload"`
}
