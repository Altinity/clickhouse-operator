package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/altinity/clickhouse-operator/pkg/plugin/backup"

	"google.golang.org/grpc"
)

// BackupServer implements pb.BackupServer
type BackupServer struct {
	pb.UnimplementedBackupServer
}

// Backup handles incoming BackupRequest calls
func (s *BackupServer) Backup(ctx context.Context, req *pb.BackupRequest) (*pb.BackupResult, error) {
	// Log what we received
	log.Printf("Received Backup request: chi=%d bytes, backup=%d bytes, params=%v",
		len(req.ChiDefinition), len(req.BackupDefinition), req.Parameters)

	// Mock result
	start := time.Now().Unix()
	time.Sleep(2 * time.Second) // simulate work
	stop := time.Now().Unix()

	result := &pb.BackupResult{
		BackupId:   "mock-backup-123",
		BackupName: "example-backup",
		StartedAt:  start,
		StoppedAt:  stop,
		Metadata: map[string]string{
			"status": "success",
			"note":   "this is a mock backup",
		},
	}

	return result, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBackupServer(grpcServer, &BackupServer{})

	fmt.Println("Mock Backup gRPC server running on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
