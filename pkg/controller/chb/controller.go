package chb

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChb "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-backup.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	pb "github.com/altinity/clickhouse-operator/pkg/plugin/backup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

type Controller struct {
	client.Client
	Scheme *apiMachinery.Scheme

	namer interfaces.INameManager
	kube  interfaces.IKube
}

func (c *Controller) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	fmt.Println("Reconcile called for request:", req)
	log.Info("inside backup reconcile", "request", req)

	newChb := &apiChb.ClickHouseBackup{}
	if err := c.Client.Get(ctx, req.NamespacedName, newChb); err != nil {
		if apiErrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			// Return and don't requeue
			return ctrl.Result{}, nil
		}
		// Return and requeue
		return ctrl.Result{}, err
	}

	//TODO need to rethink for already in progress backup
	defer c.Status().Update(ctx, newChb)
	if len(newChb.Status.State) > 0 {
		newChb.Status.State = "Failed"
		return ctrl.Result{}, nil
	}

	backupChi, err := chop.Get().ChopClient().ClickhouseV1().ClickHouseInstallations(req.Namespace).
		Get(ctx, newChb.Spec.ClickhouseInstallation.Name, meta.GetOptions{})
	if err != nil {
		return ctrl.Result{}, err
	}

	chiDefinition, err := json.Marshal(backupChi)
	if err != nil {
		return ctrl.Result{}, err
	}

	backupDefinition, err := json.Marshal(newChb)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Example request
	backupReq := &pb.BackupRequest{
		ChiDefinition:    chiDefinition,
		BackupDefinition: backupDefinition,
		Parameters:       map[string]string{},
	}

	//TODO write a plugin discovery controller
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("did not connect: %v", err)
		return ctrl.Result{}, err
	}
	defer conn.Close()
	backupClient := pb.NewBackupClient(conn)
	newChb.Status.State = "InProgress"
	c.Status().Update(ctx, newChb)
	resp, err := backupClient.Backup(ctx, backupReq)
	fmt.Printf("Backup started: %s at %d\n", resp.BackupId, resp.StartedAt)
	if err != nil {
		newChb.Status.State = "Failed"
		log.Error("could not trigger backup: %v", err)
		return ctrl.Result{}, err
	}
	newChb.Status.State = "Completed"
	newChb.Status.BackupId = resp.BackupId
	newChb.Status.BackupName = resp.BackupName
	newChb.Status.StartedAt = meta.Time{Time: time.Unix(resp.StartedAt, 0)}
	newChb.Status.StoppedAt = meta.Time{Time: time.Unix(resp.StoppedAt, 0)}

	return ctrl.Result{}, nil
}
