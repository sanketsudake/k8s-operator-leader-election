package leaderelect

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
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
	client    kubernetes.Interface
	namespace string
	leaseName string
	identity  string
	isLeader  atomic.Bool
}

func NewActivePassiveElector(client kubernetes.Interface, ns, name, id string) Elector {
	return &activePassiveElector{
		client:    client,
		namespace: ns,
		leaseName: name,
		identity:  id,
	}
}

func (e *activePassiveElector) Start(ctx context.Context) error {
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

func (e *activePassiveElector) IsLeader(_ string) bool {
	return e.isLeader.Load()
}

// --- 2. Sharded Strategy ---

type shardedElector struct {
	totalBuckets uint32
	myBucketID   uint32
}

func NewShardedElector(totalBuckets, myBucketID uint32) Elector {
	return &shardedElector{
		totalBuckets: totalBuckets,
		myBucketID:   myBucketID,
	}
}

func (e *shardedElector) Start(ctx context.Context) error {
	// Static sharding requires no background coordination loop.
	// (Dynamic lease-based sharding would combine this with ActivePassive logic per bucket).
	return nil
}

func (e *shardedElector) IsLeader(key string) bool {
	if e.totalBuckets == 0 {
		return false
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	bucket := h.Sum32() % e.totalBuckets
	return bucket == e.myBucketID
}

// --- 3. StatefulSet Deterministic Strategy ---

type statefulSetElector struct {
	leaderOrdinal string
}

func NewStatefulSetElector(leaderOrdinal string) Elector {
	return &statefulSetElector{
		leaderOrdinal: leaderOrdinal, // usually "0"
	}
}

func (e *statefulSetElector) Start(ctx context.Context) error {
	// No background API communication required.
	return nil
}

func (e *statefulSetElector) IsLeader(_ string) bool {
	hostname := os.Getenv("HOSTNAME")
	// e.g., if hostname is "my-operator-0" and leaderOrdinal is "0"
	return strings.HasSuffix(hostname, "-"+e.leaderOrdinal)
}
