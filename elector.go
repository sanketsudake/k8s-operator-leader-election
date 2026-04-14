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

// ShardOwner is an optional interface implemented by sharded electors
// to expose which buckets the current instance owns.
type ShardOwner interface {
	OwnedBuckets() []uint32
}

// --- 2. Dynamic Sharded Strategy ---
//
// Each pod competes for ALL N bucket leases. A pod becomes leader for
// bucket B when it wins lease "<lockPrefix>-B". This means:
//
//   - M = N pods: each pod wins ~1 bucket (steady state).
//   - M < N pods: some pods hold multiple buckets.
//   - M > N pods: at most N pods are active leaders; extras are hot-standby.
//
// On pod crash/scale-down the Kubernetes lease expires and a surviving
// pod acquires it automatically — no external rebalance controller needed.

type dynamicShardedElector struct {
	totalBuckets uint32
	leases       []*leaseElector
}

func NewDynamicShardedElector(client kubernetes.Interface, ns, lockPrefix, id string, totalBuckets uint32) Elector {
	leases := make([]*leaseElector, totalBuckets)
	for i := range totalBuckets {
		leaseName := fmt.Sprintf("%s-%d", lockPrefix, i)
		leases[i] = newLeaseElector(client, ns, leaseName, id)
	}
	return &dynamicShardedElector{
		totalBuckets: totalBuckets,
		leases:       leases,
	}
}

func (e *dynamicShardedElector) Start(ctx context.Context) error {
	if e.totalBuckets == 0 {
		return fmt.Errorf("totalBuckets must be greater than 0")
	}
	for i, l := range e.leases {
		if err := l.Start(ctx); err != nil {
			return fmt.Errorf("failed to start lease for bucket %d: %w", i, err)
		}
	}
	return nil
}

func (e *dynamicShardedElector) IsLeader(key string) bool {
	if e.totalBuckets == 0 {
		return false
	}
	bucket := bucketForKey(key, e.totalBuckets)
	return e.leases[bucket].IsLeader()
}

// OwnedBuckets returns the list of bucket IDs this pod currently leads.
func (e *dynamicShardedElector) OwnedBuckets() []uint32 {
	var owned []uint32
	for i, l := range e.leases {
		if l.IsLeader() {
			owned = append(owned, uint32(i))
		}
	}
	return owned
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
