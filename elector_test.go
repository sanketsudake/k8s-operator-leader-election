package leaderelect

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestActivePassiveElector_EnvTest(t *testing.T) {
	clientset := newTestClientset(t)
	elector := NewActivePassiveElector(clientset, "default", "active-passive-lock", "pod-1")

	require.NoError(t, elector.Start(t.Context()))
	require.Eventually(t, func() bool {
		return elector.IsLeader("ignored")
	}, 10*time.Second, 200*time.Millisecond)
}

func TestShardedElector_AssignedBucketLeadership(t *testing.T) {
	clientset := newTestClientset(t)
	elector := NewShardedElector(clientset, "default", "sharded-lock", "pod-1", 3, 1)

	assignedKey := findKeyForBucket(t, 3, 1)
	nonAssignedKey := findKeyForBucket(t, 3, 2)

	require.False(t, elector.IsLeader(assignedKey))
	require.NoError(t, elector.Start(t.Context()))

	require.Eventually(t, func() bool {
		return elector.IsLeader(assignedKey)
	}, 10*time.Second, 200*time.Millisecond)
	require.False(t, elector.IsLeader(nonAssignedKey))
}

func TestShardedElector_ContendersAndFailover(t *testing.T) {
	clientset := newTestClientset(t)
	key := findKeyForBucket(t, 3, 1)

	electorA := NewShardedElector(clientset, "default", "sharded-failover-lock", "pod-a", 3, 1)
	electorB := NewShardedElector(clientset, "default", "sharded-failover-lock", "pod-b", 3, 1)

	ctxA, cancelA := context.WithCancel(t.Context())
	ctxB, cancelB := context.WithCancel(t.Context())
	t.Cleanup(cancelA)
	t.Cleanup(cancelB)

	require.NoError(t, electorA.Start(ctxA))
	require.NoError(t, electorB.Start(ctxB))

	var aLeader, bLeader bool
	require.Eventually(t, func() bool {
		aLeader = electorA.IsLeader(key)
		bLeader = electorB.IsLeader(key)
		return aLeader != bLeader
	}, 15*time.Second, 200*time.Millisecond)

	if aLeader {
		cancelA()
		require.Eventually(t, func() bool {
			return electorB.IsLeader(key)
		}, 20*time.Second, 250*time.Millisecond)
		return
	}

	cancelB()
	require.Eventually(t, func() bool {
		return electorA.IsLeader(key)
	}, 20*time.Second, 250*time.Millisecond)
}

func TestShardedElector_InvalidConfiguration(t *testing.T) {
	clientset := newTestClientset(t)

	zeroBuckets := NewShardedElector(clientset, "default", "sharded-invalid-lock", "pod-1", 0, 0)
	err := zeroBuckets.Start(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "totalBuckets")

	invalidBucket := NewShardedElector(clientset, "default", "sharded-invalid-lock", "pod-1", 3, 3)
	err = invalidBucket.Start(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "myBucketID")
}

func TestStatefulSetElector_ContendersAndFailover(t *testing.T) {
	clientset := newTestClientset(t)

	electorA := NewStatefulSetElector(clientset, "default", "statefulset-lock", "pod-a")
	electorB := NewStatefulSetElector(clientset, "default", "statefulset-lock", "pod-b")

	ctxA, cancelA := context.WithCancel(t.Context())
	ctxB, cancelB := context.WithCancel(t.Context())
	t.Cleanup(cancelA)
	t.Cleanup(cancelB)

	require.NoError(t, electorA.Start(ctxA))
	require.NoError(t, electorB.Start(ctxB))

	var aLeader, bLeader bool
	require.Eventually(t, func() bool {
		aLeader = electorA.IsLeader("")
		bLeader = electorB.IsLeader("")
		return aLeader != bLeader
	}, 15*time.Second, 200*time.Millisecond)

	if aLeader {
		cancelA()
		require.Eventually(t, func() bool {
			return electorB.IsLeader("")
		}, 20*time.Second, 250*time.Millisecond)
		return
	}

	cancelB()
	require.Eventually(t, func() bool {
		return electorA.IsLeader("")
	}, 20*time.Second, 250*time.Millisecond)
}

func TestStatefulSetElector_UsesHostnameWhenIdentityEmpty(t *testing.T) {
	clientset := newTestClientset(t)
	t.Setenv("HOSTNAME", "stateful-pod-2")

	elector := NewStatefulSetElector(clientset, "default", "statefulset-hostname-lock", "")
	require.NoError(t, elector.Start(t.Context()))
	require.Eventually(t, func() bool {
		return elector.IsLeader("")
	}, 10*time.Second, 200*time.Millisecond)
}

func findKeyForBucket(t *testing.T, totalBuckets, targetBucket uint32) string {
	t.Helper()
	require.NotZero(t, totalBuckets)
	require.Less(t, targetBucket, totalBuckets)

	for i := range 50_000 {
		key := fmt.Sprintf("default/workload-%d", i)
		if bucketForKey(key, totalBuckets) == targetBucket {
			return key
		}
	}

	t.Fatalf("failed to find key for bucket %d/%d", targetBucket, totalBuckets)
	return ""
}

func newTestClientset(t *testing.T) kubernetes.Interface {
	t.Helper()
	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testEnv.Stop())
	})

	clientset, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	return clientset
}
