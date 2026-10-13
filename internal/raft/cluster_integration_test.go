package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/config"
	rpc "raft/internal/rpc/gen"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestCluster manages a multi-node Raft cluster for testing
type TestCluster struct {
	Nodes       []*Node
	Clients     []rpc.ClientServiceClient
	Conns       []*grpc.ClientConn
	Ports       []string
	TempDir     string
	NodeDirs    []string
	cancels     []context.CancelFunc
	leaderIdx   int
	grpcServers []*grpc.Server
	listeners   []net.Listener
}

func setPayload(key, value string) []byte {
	keyEnd := uint32(8 + len(key))
	valueEnd := keyEnd + uint32(len(value))
	payload := make([]byte, valueEnd)
	binary.BigEndian.PutUint32(payload[0:4], keyEnd)
	binary.BigEndian.PutUint32(payload[4:8], valueEnd)
	copy(payload[8:keyEnd], key)
	copy(payload[keyEnd:valueEnd], value)
	return payload
}

func NewTestCluster(t *testing.T, nodeCount int) *TestCluster {
	t.Helper()

	// Create temp directory for log files
	tempDir, err := os.MkdirTemp("", "raft-test-*")
	require.NoError(t, err)

	// Reserve an ephemeral listener for each node before constructing the cluster
	// configuration. This avoids collisions with other local test runs.
	ports := make([]string, nodeCount)
	listeners := make([]net.Listener, nodeCount)
	for i := 0; i < nodeCount; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = listener
		ports[i] = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	}

	// Create seed config
	seeds := make([]config.Seed, nodeCount)
	for i, port := range ports {
		seeds[i] = config.Seed{
			Id:   fmt.Sprintf("node-%d", i),
			Addr: "127.0.0.1:" + port,
			Port: port,
		}
	}

	var nodes []*Node
	var clients []rpc.ClientServiceClient
	var conns []*grpc.ClientConn
	var cancels []context.CancelFunc
	var grpcServers []*grpc.Server
	nodeDirs := make([]string, nodeCount)

	// Start all nodes
	for i, port := range ports {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)

		// Each node needs its own persistent log file even though this test hosts
		// all nodes in one process.
		nodeDir := filepath.Join(tempDir, fmt.Sprintf("node-%d", i))
		require.NoError(t, os.Mkdir(nodeDir, 0755))
		nodeDirs[i] = nodeDir
		oldDir, _ := os.Getwd()
		require.NoError(t, os.Chdir(nodeDir))

		node, logger, err := InitNode(ctx, port, seeds)
		require.NoError(t, err)

		// Start gRPC server for this node
		grpcServer := grpc.NewServer()
		rpc.RegisterPeerServiceServer(grpcServer, node)
		rpc.RegisterClientServiceServer(grpcServer, node)
		go func(listener net.Listener, server *grpc.Server) {
			_ = server.Serve(listener)
		}(listeners[i], grpcServer)

		logger.Info("Node started", "id", node.id, "port", port)

		require.NoError(t, os.Chdir(oldDir))
		nodes = append(nodes, node)
		grpcServers = append(grpcServers, grpcServer)

		// Create client connection
		conn, err := grpc.NewClient("127.0.0.1:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		conns = append(conns, conn)
		clients = append(clients, rpc.NewClientServiceClient(conn))
	}

	cluster := &TestCluster{
		Nodes:       nodes,
		Clients:     clients,
		Conns:       conns,
		Ports:       ports,
		TempDir:     tempDir,
		NodeDirs:    nodeDirs,
		cancels:     cancels,
		grpcServers: grpcServers,
		listeners:   listeners,
	}

	// Connect all peers now that servers are running
	for i, node := range nodes {
		err := node.ConnectPeers(seeds, ports[i])
		require.NoError(t, err)
	}

	// Wait for leader election
	leaderIdx := cluster.waitForLeader(t, 10*time.Second)
	cluster.leaderIdx = leaderIdx

	return cluster
}

func (c *TestCluster) waitForLeader(t *testing.T, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, node := range c.Nodes {
			if node != nil && node.role == Leader {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("No leader elected within timeout")
	return -1
}

func (c *TestCluster) GetLeader() *Node {
	return c.Nodes[c.leaderIdx]
}

func (c *TestCluster) GetLeaderClient() rpc.ClientServiceClient {
	return c.Clients[c.leaderIdx]
}

func (c *TestCluster) WaitForCommit(t *testing.T, expectedIndex uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := c.GetLeader()
		if leader != nil && leader.commitIndex >= expectedIndex && leader.lastApplied >= expectedIndex {
			// Verify all followers have applied
			allApplied := true
			for _, node := range c.Nodes {
				if node != nil && node.lastApplied < expectedIndex {
					allApplied = false
					break
				}
			}
			if allApplied {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Entry %d not committed within timeout", expectedIndex)
}

func (c *TestCluster) Shutdown() {
	for _, cancel := range c.cancels {
		cancel()
	}
	for _, conn := range c.Conns {
		if conn != nil {
			conn.Close()
		}
	}
	for _, server := range c.grpcServers {
		if server != nil {
			server.GracefulStop()
		}
	}
	for _, listener := range c.listeners {
		if listener != nil {
			listener.Close()
		}
	}
	os.RemoveAll(c.TempDir)
}

func (c *TestCluster) KillNode(t *testing.T, idx int) {
	t.Helper()
	c.cancels[idx]()
	if c.Conns[idx] != nil {
		c.Conns[idx].Close()
		c.Conns[idx] = nil
	}
	if c.grpcServers[idx] != nil {
		c.grpcServers[idx].Stop()
		c.grpcServers[idx] = nil
	}
	if c.listeners[idx] != nil {
		c.listeners[idx].Close()
		c.listeners[idx] = nil
	}
	c.Nodes[idx] = nil
	c.Clients[idx] = nil
}

func (c *TestCluster) RestartNode(t *testing.T, idx int) {
	t.Helper()
	c.KillNode(t, idx)
	port := c.Ports[idx]
	seeds := make([]config.Seed, len(c.Ports))
	for i, p := range c.Ports {
		seeds[i] = config.Seed{
			Id:   fmt.Sprintf("node-%d", i),
			Addr: "127.0.0.1:" + p,
			Port: p,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancels[idx] = cancel

	oldDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(c.NodeDirs[idx]))
	node, _, err := InitNode(ctx, port, seeds)
	require.NoError(t, err)
	require.NoError(t, os.Chdir(oldDir))

	c.Nodes[idx] = node
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	rpc.RegisterPeerServiceServer(grpcServer, node)
	rpc.RegisterClientServiceServer(grpcServer, node)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	c.listeners[idx] = listener
	c.grpcServers[idx] = grpcServer

	conn, err := grpc.NewClient("127.0.0.1:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	c.Conns[idx] = conn
	c.Clients[idx] = rpc.NewClientServiceClient(conn)
	require.NoError(t, node.ConnectPeers(seeds, port))
}

// TestBasicElection verifies that a cluster elects a leader
func TestBasicElection(t *testing.T) {
	cluster := NewTestCluster(t, 3)
	defer cluster.Shutdown()

	leader := cluster.GetLeader()
	assert.Equal(t, Leader, leader.role)
	assert.NotEmpty(t, leader.id)

	// Verify only one leader
	leaderCount := 0
	for _, node := range cluster.Nodes {
		if node.role == Leader {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount)
}
