package raft

import (
	"context"
	"net"
	"os"
	"os/signal"
	"raft/internal/config"
	rpc "raft/internal/rpc/gen"
	"syscall"

	"google.golang.org/grpc"
)

func Init(port string) error {
	seeds := config.FetchSeeds()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	n, logger, err := InitNode(ctx, port, seeds)
	if err != nil {
		return err
	}
	go func() {
		<-sig
		logger.Info("Received cancellation signal, shuting down.....")
		cancel()
	}()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	rpc.RegisterPeerServiceServer(grpcServer, n)

	logger.Info("Raft Node is listening", "port", port)

	if err := grpcServer.Serve(listener); err != nil {
		return err
	}
	return nil
}
