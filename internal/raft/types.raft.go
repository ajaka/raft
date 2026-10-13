package raft

import rpc "raft/internal/rpc/gen"

type ROLE string

const (
	Leader    ROLE = "Leader"
	Follower  ROLE = "Follower"
	Candidate ROLE = "Candidate"
)

type EventType string

const (
	AppendEntries EventType = "AppendEntries"
	RequestVote   EventType = "RequestVote"
	MakeProposal  EventType = "MakeProposal"
)

type RequestVoteEvent struct {
	payload *rpc.RequestVoteRequest
	send    chan *rpc.RequestVoteResponse
}

type AppendEntriesEvent struct {
	payload *rpc.AppendEntriesRequest
	send    chan *rpc.AppendEntriesResponse
}

type MakeProposalEvent struct {
	payload *rpc.ProposalRequest
	send    chan *rpc.ProposalResponse
}

type Event struct {
	eventType EventType
	payload   any
}
