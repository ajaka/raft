package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"raft/internal/config"
	"raft/internal/db"
	"raft/internal/log"
	rpc "raft/internal/rpc/gen"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	Conn        rpc.PeerServiceClient
	Next_index  uint64
	Match_index uint64
}

type Node struct {
	rpc.UnimplementedPeerServiceServer
	rpc.UnimplementedClientServiceServer
	id          string
	leaderId    string
	votedFor    string
	role        ROLE
	term        uint64
	peers       map[string]*Peer
	clusterSize int
	logger      *slog.Logger
	lastApplied uint64
	commitIndex uint64
	log         *log.Log
	db          *db.KVStore
	timer       *time.Timer
	conns       []*grpc.ClientConn
	eventChan   chan Event
	leaderReady bool // true after peer init completes
}

func InitNode(ctx context.Context, port string, seeds []config.Seed) (*Node, *slog.Logger, error) {

	n := Node{
		role:        Follower,
		term:        0,
		peers:       make(map[string]*Peer),
		eventChan:   make(chan Event),
		leaderId:    "",
		votedFor:    "",
		lastApplied: 0,
		commitIndex: 0,
		db:          db.InitDB(),
		clusterSize: len(seeds),
	}

	var me config.Seed

	for _, s := range seeds {
		if s.Port == port {
			me = s
			continue
		}
	}
	log, err := log.InitLog(me.Filename)
	if err != nil {
		return nil, nil, err
	}
	n.log = log
	logger := slog.Default().With("ID", me.Id)
	n.logger = logger
	n.id = me.Id

	n.timer = time.NewTimer(time.Duration(n.RandomTimeoutWithSeed()) * time.Millisecond)

	n.Init(ctx)
	return &n, logger, nil
}

func (n *Node) ConnectPeers(seeds []config.Seed, port string) error {
	for i, s := range seeds {
		if s.Port == port {
			continue
		}
		p, err := n.BuildNewPeer(s.Addr)
		if err != nil {
			return err
		}
		peer := &Peer{
			Conn:        p,
			Next_index:  1,
			Match_index: 0,
		}
		n.peers[fmt.Sprintf("%v", i)] = peer
	}
	return nil
}

func (n *Node) Init(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				n.handleShutdown()
				return
			case <-n.timer.C:
				n.BecomeCandidate(ctx)
			case e := <-n.eventChan:
				n.HandleEvent(ctx, e)
			}
		}
	}()
}

func (n *Node) handleShutdown() {
	for _, c := range n.conns {
		c.Close()
	}
}

func (n *Node) HandleEvent(ctx context.Context, e Event) {
	switch e.eventType {
	case RequestVote:
		reqPayload := e.payload.(RequestVoteEvent)
		n.HandleVoteRequest(reqPayload)
		return
	case AppendEntries:
		reqPayload := e.payload.(AppendEntriesEvent)
		n.HandleAppendEntriesEvent(reqPayload)
		return
	case MakeProposal:
		reqPayload := e.payload.(MakeProposalEvent)
		n.handleProposal(ctx, reqPayload)
	default:
		n.logger.Warn("Received invalid or unhandled event type ", "event", e.eventType)
	}
}

func (n *Node) resetTimer() {
	n.timer.Reset(time.Duration(n.RandomTimeoutWithSeed()) * time.Millisecond)
}

func (n *Node) becomeFollower(term uint64, leaderID string) {
	if term > n.term {
		n.votedFor = ""
	}
	n.role = Follower
	n.term = term
	n.leaderId = leaderID
	n.leaderReady = false
	n.resetTimer()
}

func (n *Node) HandleAppendEntriesEvent(ar AppendEntriesEvent) {
	n.logger.Debug("Received AppendEntries", "leaderId", ar.payload.LeaderId, "term", ar.payload.Term, "prevLogIndex", ar.payload.LastLogIndex, "prevLogTerm", ar.payload.LastLogTerm, "entriesCount", len(ar.payload.Entries), "leaderCommit", ar.payload.LeaderCommit)
	resp := rpc.AppendEntriesResponse{
		LastLogIndex: n.log.GetLastIndex(),
		LastLogTerm:  n.log.GetLastTerm(),
		Ok:           false,
		LeaderId:     n.leaderId,
		Term:         n.term,
	}
	if ar.payload.Term < n.term {
		n.logger.Debug("Rejecting AppendEntries: stale term", "leaderTerm", ar.payload.Term, "myTerm", n.term)
		ar.send <- &resp
		return
	}
	n.becomeFollower(ar.payload.Term, ar.payload.LeaderId)
	resp.Term = n.term
	resp.LeaderId = n.leaderId

	term, ok := n.log.TermAt(ar.payload.LastLogIndex)
	if ar.payload.LastLogIndex != 0 && (!ok || ar.payload.LastLogTerm != term) {
		n.logger.Debug("Rejecting AppendEntries: log mismatch", "prevLogIndex", ar.payload.LastLogIndex, "expectedTerm", ar.payload.LastLogTerm, "actualTerm", term, "hasIndex", ok)
		ar.send <- &resp
		return
	}

	if ar.payload.LeaderCommit > n.commitIndex {
		n.commitIndex = min(ar.payload.LeaderCommit, n.log.GetLastIndex())
		n.logger.Debug("Updated commitIndex", "commitIndex", n.commitIndex)
	}

	var entries []log.Entry
	for _, e := range ar.payload.Entries {
		entries = append(entries, log.Entry{
			Index:   e.Index,
			Term:    e.Term,
			Type:    e.Type,
			Payload: e.Payload,
		})
	}
	err := n.log.AppendEntries(entries)

	if err != nil {
		n.logger.Debug("AppendEntries failed", "error", err)
		ar.send <- &resp
		return
	}

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		ok := n.Makeapply(n.lastApplied)
		if !ok {
			break
		}
	}

	resp.Ok = true
	resp.Term = n.term
	resp.LeaderId = n.leaderId
	resp.LastLogIndex = n.log.GetLastIndex()
	resp.LastLogTerm = n.log.GetLastTerm()

	n.logger.Debug("AppendEntries accepted", "lastLogIndex", resp.LastLogIndex, "lastLogTerm", resp.LastLogTerm)
	ar.send <- &resp
}

func (n *Node) Makeapply(index uint64) bool {
	entry, ok := n.log.GetEntry(index)
	if !ok {
		return ok
	}
	n.db.Apply(entry)
	return true
}

func (n *Node) HandleVoteRequest(vr RequestVoteEvent) {
	resp := rpc.RequestVoteResponse{
		Term: n.term,
		Vote: false,
	}
	if vr.payload.Term < n.term {
		vr.send <- &resp
		return
	}
	if vr.payload.Term > n.term {
		n.becomeFollower(vr.payload.Term, "")
	}
	if n.role != Follower {
		resp.Term = n.term
		vr.send <- &resp
		return
	}

	valid := false
	if (n.votedFor == "" || n.votedFor == vr.payload.CandidateId) && ((vr.payload.LastLogTerm > n.log.GetLastTerm()) || (vr.payload.LastLogTerm == n.log.GetLastTerm() && vr.payload.LastLogIndex >= n.log.GetLastIndex())) {
		valid = true
	}

	if valid {
		resp.Vote = true
		n.resetTimer()
		n.votedFor = vr.payload.CandidateId

	}
	resp.Term = n.term
	vr.send <- &resp
}

func (n *Node) RandomTimeoutWithSeed() uint16 {
	seed := int64(0)
	for _, b := range []byte(n.id) {
		seed = seed*31 + int64(b)
	}
	min := 350.0
	max := 600.0
	r := rand.New(rand.NewSource(seed))
	return uint16(r.Float64()*(max-min) + min)
}
func (n *Node) BecomeCandidate(ctx context.Context) {
	n.logger.Info("Timeout exceeded, becoming candidate")
	n.role = Candidate
	n.term++
	n.leaderReady = false // Reset on new election
	voted := n.requestVotes(ctx)
	if voted/float64(n.clusterSize) > 0.5 {
		n.role = Leader
		n.logger.Info("Became leader, initializing peer state", "lastLogIndex", n.log.GetLastIndex())
		for id := range n.peers {
			peer := n.peers[id]
			peer.Next_index = n.log.GetLastIndex() + 1
			peer.Match_index = 0
			n.logger.Info("Initialized peer", "peerId", id, "nextIndex", peer.Next_index)
		}
		n.leaderReady = true
		n.logger.Info("Leader ready for proposals", "leaderReady", n.leaderReady)
		n.SendEntries(ctx, []*rpc.Entry{})
		n.stopTimer()
		go n.runLeaderLoop(ctx)
	} else {
		n.becomeFollower(n.term, "")
	}
}

func (n *Node) stopTimer() {
	if !n.timer.Stop() {
		select {
		case <-n.timer.C:
		default:
		}
	}
}

func (n *Node) runLeaderLoop(ctx context.Context) {
	ticker := time.NewTicker(350 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			n.handleShutdown()
			return
		case <-ticker.C:
			if n.role != Leader {
				return
			}
			n.SendEntries(ctx, []*rpc.Entry{})
		}
	}
}

func (n *Node) handleProposal(ctx context.Context, mr MakeProposalEvent) {
	n.logger.Debug("handleProposal started", "role", n.role, "term", n.term, "leaderReady", n.leaderReady)
	resp := rpc.ProposalResponse{
		Ok:       false,
		Term:     n.log.GetLastTerm(),
		LeaderId: n.leaderId,
	}
	if n.role != Leader || !n.leaderReady {
		n.logger.Warn("Proposal rejected: not leader or not ready", "role", n.role, "leaderReady", n.leaderReady)
		mr.send <- &resp
		return
	}
	logEntry := log.Entry{
		Index:   n.log.GetLastIndex() + 1,
		Term:    n.term,
		Type:    mr.payload.Cmd,
		Payload: mr.payload.Payload,
	}
	err := n.log.AppendEntries([]log.Entry{logEntry})
	if err != nil {
		mr.send <- &resp
		return
	}

	respChan := make(chan bool, len(n.peers))
	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	for id, p := range n.peers {
		maxAttempts := 3
		go func(id string, p *Peer) {
			n.logger.Info("Replication goroutine started", "peerId", id, "initialNextIndex", p.Next_index)
			attempts := 0
			for {
				select {
				case <-done:
					n.logger.Debug("Replication goroutine exiting (done)", "peerId", id)
					return
				default:
				}
				attempts++
				entries := n.log.GetEntries(p.Next_index)
				prevLogIndex := p.Next_index - 1
				prevLogTerm := uint64(0)
				if prevLogIndex > 0 {
					if term, ok := n.log.TermAt(prevLogIndex); ok {
						prevLogTerm = term
					}
				}
				n.logger.Debug("Sending AppendEntries for replication", "peerId", id, "prevLogIndex", prevLogIndex, "entriesCount", len(entries), "attempt", attempts)
				appendEntriesResp, err := p.Conn.AppendEntriesEvent(ctx, &rpc.AppendEntriesRequest{
					Term:         n.term,
					LeaderId:     n.id,
					LastLogIndex: prevLogIndex,
					LastLogTerm:  prevLogTerm,
					LeaderCommit: n.commitIndex,
					Entries:      LogEntryToRpcEntry(entries),
				})
				if err != nil {
					if attempts <= maxAttempts {
						n.logger.Warn("Peer request returned with a connection error, retrying...", "error", err, "peerId", id, "attempt", attempts)
						continue
					}
					n.logger.Error("Peer request failed", "error", err, "peerId", id)
					select {
					case respChan <- false:
					case <-done:
					}
					break
				}
				n.logger.Debug("Received AppendEntries response", "peerId", id, "ok", appendEntriesResp.Ok, "term", appendEntriesResp.Term, "lastLogIndex", appendEntriesResp.LastLogIndex)
				if !appendEntriesResp.Ok {
					if p.Next_index > 1 {
						p.Next_index -= 1
						n.logger.Debug("Decrementing NextIndex, retrying", "peerId", id, "nextIndex", p.Next_index)
						continue
					}
					n.logger.Error("Invalid state reached ", "peerId", p)
					select {
					case respChan <- false:
					case <-done:
					}
					break
				}
				p.Match_index = appendEntriesResp.LastLogIndex
				p.Next_index += 1
				n.logger.Debug("Replication successful", "peerId", id, "matchIndex", p.Match_index, "nextIndex", p.Next_index)
				select {
				case respChan <- true:
				case <-done:
				}
				break
			}
		}(id, p)
	}
	agreed := 1.0
	yep := false
	responses := 0
	for responses < len(n.peers) {
		x := <-respChan
		responses++
		if x {
			agreed += 1
			if agreed/float64(n.clusterSize) > 0.5 {
				yep = true
				break
			}
		}
	}
	close(done) // Signal remaining goroutines to exit
	cancel()
	if !yep {
		mr.send <- &resp
		return
	}
	n.commitIndex = logEntry.Index
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		ok := n.Makeapply(n.lastApplied)
		if !ok {
			break
		}
	}
	resp.Ok = true
	mr.send <- &resp
}

func (n *Node) SendEntries(ctx context.Context, entries []*rpc.Entry) {
	for _, p := range n.peers {
		prevLogIndex := p.Next_index - 1
		prevLogTerm := uint64(0)
		if prevLogIndex > 0 {
			if term, ok := n.log.TermAt(prevLogIndex); ok {
				prevLogTerm = term
			}
		}
		p.Conn.AppendEntriesEvent(ctx, &rpc.AppendEntriesRequest{
			Term:         n.term,
			LeaderId:     n.id,
			LastLogIndex: prevLogIndex,
			LastLogTerm:  prevLogTerm,
			LeaderCommit: n.commitIndex,
			Entries:      entries,
		})
	}
}

func (n *Node) requestVotes(ctx context.Context) float64 {
	voted := 1.0
	for _, p := range n.peers {
		resp, err := p.Conn.RequestVoteEvent(ctx, &rpc.RequestVoteRequest{
			CandidateId:  n.id,
			Term:         n.term,
			LastLogIndex: n.log.GetLastIndex(),
			LastLogTerm:  n.log.GetLastTerm(),
		})
		if err != nil {
			continue
		}
		if resp.Vote {
			voted++
		}
	}
	return voted
}

func (n *Node) RequestVoteEvent(ctx context.Context, vr *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error) {
	n.logger.Info("Received Vote request from peer")

	respChan := make(chan *rpc.RequestVoteResponse)
	defer close(respChan)

	e := Event{
		eventType: RequestVote,
		payload: RequestVoteEvent{
			payload: vr,
			send:    respChan,
		},
	}

	n.eventChan <- e
	resp := <-respChan
	return resp, nil
}

func (n *Node) MakeProposal(ctx context.Context, mp *rpc.ProposalRequest) (*rpc.ProposalResponse, error) {
	n.logger.Info("Received Proposal request from client")

	respChan := make(chan *rpc.ProposalResponse)
	defer close(respChan)

	e := Event{
		eventType: MakeProposal,
		payload: MakeProposalEvent{
			payload: mp,
			send:    respChan,
		},
	}
	n.eventChan <- e
	resp := <-respChan
	return resp, nil
}

func (n *Node) AppendEntriesEvent(ctx context.Context, ar *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error) {
	n.logger.Info("Received AppendEntries request from peer")

	respChan := make(chan *rpc.AppendEntriesResponse)
	defer close(respChan)

	e := Event{
		eventType: AppendEntries,
		payload: AppendEntriesEvent{
			payload: ar,
			send:    respChan,
		},
	}
	n.eventChan <- e
	resp := <-respChan
	return resp, nil
}

func (n *Node) BuildNewPeer(addr string) (rpc.PeerServiceClient, error) {
	maxAttempts := 3
	cur := 0
	for {
		cur++
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			if cur <= maxAttempts {
				time.Sleep(250 * time.Millisecond)
				continue
			} else {
				return nil, err
			}
		}
		n.conns = append(n.conns, conn)
		c := rpc.NewPeerServiceClient(conn)
		return c, nil

	}
}

func LogEntryToRpcEntry(e []log.Entry) []*rpc.Entry {
	var results []*rpc.Entry
	for _, entry := range e {
		rpcEntry := rpc.Entry{
			Index:   entry.Index,
			Term:    entry.Term,
			Type:    entry.Type,
			Payload: entry.Payload,
		}
		results = append(results, &rpcEntry)
	}
	return results
}
