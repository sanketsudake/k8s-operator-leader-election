package leaderelect

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Strategy defines the type of leader election
type Strategy string

const (
	ActivePassive Strategy = "ActivePassive"
	Sharded       Strategy = "Sharded"
	StatefulSet   Strategy = "StatefulSet"
)

// Elector is the common interface for all leader election strategies.
type Elector interface {
	// Start begins the election process (non-blocking).
	Start(ctx context.Context) error
	// IsLeader returns true if this instance is the leader for the given key.
	// For ActivePassive and StatefulSet, the key is ignored.
	IsLeader(key string) bool
}

// --- 1. Active-Passive Strategy ---

type activePassiveElector struct {
	lease *leaseElector
}

func NewActivePassiveElector(client kubernetes.Interface, ns, name, id string) Elector {
	return &activePassiveElector{
		lease: newLeaseElector(client, ns, name, id),
	}
}

func (e *activePassiveElector) Start(ctx context.Context) error {
	return e.lease.Start(ctx)
}

func (e *activePassiveElector) IsLeader(_ string) bool {
	return e.lease.IsLeader()
}

type leaseElector struct {
	client    kubernetes.Interface
	namespace string
	leaseName string
	identity  string
	isLeader  atomic.Bool
}

func newLeaseElector(client kubernetes.Interface, namespace, leaseName, identity string) *leaseElector {
	return &leaseElector{
		client:    client,
		namespace: namespace,
		leaseName: leaseName,
		identity:  identity,
	}
}

func (e *leaseElector) Start(ctx context.Context) error {
	if e.client == nil {
		return fmt.Errorf("client cannot be nil")
	}
	if e.namespace == "" {
		return fmt.Errorf("namespace cannot be empty")
	}
	if e.leaseName == "" {
		return fmt.Errorf("leaseName cannot be empty")
	}
	if e.identity == "" {
		return fmt.Errorf("identity cannot be empty")
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      e.leaseName,
			Namespace: e.namespace,
		},
		Client: e.client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: e.identity,
		},
	}

	config := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				e.isLeader.Store(true)
			},
			OnStoppedLeading: func() {
				e.isLeader.Store(false)
			},
		},
	}

	elector, err := leaderelection.NewLeaderElector(config)
	if err != nil {
		return fmt.Errorf("failed to create elector: %w", err)
	}

	go elector.Run(ctx)
	return nil
}

func (e *leaseElector) IsLeader() bool {
	return e.isLeader.Load()
}

// --- 2. Sharded Strategy ---

type shardedElector struct {
	totalBuckets uint32
	myBucketID   uint32
	lease        *leaseElector
}

func NewShardedElector(client kubernetes.Interface, ns, lockPrefix, id string, totalBuckets, myBucketID uint32) Elector {
	leaseName := fmt.Sprintf("%s-%d", lockPrefix, myBucketID)
	return &shardedElector{
		totalBuckets: totalBuckets,
		myBucketID:   myBucketID,
		lease:        newLeaseElector(client, ns, leaseName, id),
	}
}

func (e *shardedElector) Start(ctx context.Context) error {
	if e.totalBuckets == 0 {
		return fmt.Errorf("totalBuckets must be greater than 0")
	}
	if e.myBucketID >= e.totalBuckets {
		return fmt.Errorf("myBucketID (%d) must be less than totalBuckets (%d)", e.myBucketID, e.totalBuckets)
	}
	return e.lease.Start(ctx)
}

func (e *shardedElector) IsLeader(key string) bool {
	if e.totalBuckets == 0 {
		return false
	}
	if e.myBucketID >= e.totalBuckets {
		return false
	}
	bucket := bucketForKey(key, e.totalBuckets)
	return bucket == e.myBucketID && e.lease.IsLeader()
}

func bucketForKey(key string, totalBuckets uint32) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % totalBuckets
}

// --- 3. StatefulSet Deterministic Strategy ---

type statefulSetElector struct {
	lease *leaseElector
}

func NewStatefulSetElector(client kubernetes.Interface, ns, leaseName, identity string) Elector {
	if identity == "" {
		identity = os.Getenv("HOSTNAME")
	}
	return &statefulSetElector{
		lease: newLeaseElector(client, ns, leaseName, identity),
	}
}

func (e *statefulSetElector) Start(ctx context.Context) error {
	return e.lease.Start(ctx)
}

func (e *statefulSetElector) IsLeader(_ string) bool {
	return e.lease.IsLeader()
}
