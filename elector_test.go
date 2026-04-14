package leaderelect

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func TestMain(m *testing.M) {
	log.SetLogger(zap.New(zap.UseDevMode(true)))
	os.Exit(m.Run())
}

func TestActivePassiveElector_EnvTest(t *testing.T) {
	clientset := newTestClientset(t)
	elector := NewActivePassiveElector(clientset, "default", "active-passive-lock", "pod-1")

	require.NoError(t, elector.Start(t.Context()))
	require.Eventually(t, func() bool {
		return elector.IsLeader("ignored")
	}, 10*time.Second, 200*time.Millisecond)
}

func TestDynamicShardedElector_SinglePodOwnsAllBuckets(t *testing.T) {
	clientset := newTestClientset(t)
	const totalBuckets uint32 = 3

	elector := NewDynamicShardedElector(clientset, "default", "shard-single", "pod-1", totalBuckets)
	require.NoError(t, elector.Start(t.Context()))

	owner, ok := elector.(ShardOwner)
	require.True(t, ok)

	require.Eventually(t, func() bool {
		return uint32(len(owner.OwnedBuckets())) == totalBuckets
	}, 15*time.Second, 200*time.Millisecond)

	for b := range totalBuckets {
		key := findKeyForBucket(t, totalBuckets, b)
		require.True(t, elector.IsLeader(key))
	}
}

func TestDynamicShardedElector_TwoPodsShareBuckets(t *testing.T) {
	clientset := newTestClientset(t)
	const totalBuckets uint32 = 4

	electorA := NewDynamicShardedElector(clientset, "default", "shard-share", "pod-a", totalBuckets)
	electorB := NewDynamicShardedElector(clientset, "default", "shard-share", "pod-b", totalBuckets)

	require.NoError(t, electorA.Start(t.Context()))
	require.NoError(t, electorB.Start(t.Context()))

	ownerA := electorA.(ShardOwner)
	ownerB := electorB.(ShardOwner)

	require.Eventually(t, func() bool {
		return len(ownerA.OwnedBuckets())+len(ownerB.OwnedBuckets()) == int(totalBuckets)
	}, 20*time.Second, 250*time.Millisecond)

	require.GreaterOrEqual(t, len(ownerA.OwnedBuckets()), 1)
	require.GreaterOrEqual(t, len(ownerB.OwnedBuckets()), 1)

	owned := make(map[uint32]bool)
	for _, b := range ownerA.OwnedBuckets() {
		owned[b] = true
	}
	for _, b := range ownerB.OwnedBuckets() {
		require.False(t, owned[b], "bucket %d owned by both pods", b)
		owned[b] = true
	}
	require.Len(t, owned, int(totalBuckets))
}

func TestDynamicShardedElector_FailoverRedistributes(t *testing.T) {
	clientset := newTestClientset(t)
	const totalBuckets uint32 = 3

	ctxA, cancelA := context.WithCancel(t.Context())
	t.Cleanup(cancelA)

	electorA := NewDynamicShardedElector(clientset, "default", "shard-failover", "pod-a", totalBuckets)
	electorB := NewDynamicShardedElector(clientset, "default", "shard-failover", "pod-b", totalBuckets)

	require.NoError(t, electorA.Start(ctxA))
	require.NoError(t, electorB.Start(t.Context()))

	ownerA := electorA.(ShardOwner)
	ownerB := electorB.(ShardOwner)

	require.Eventually(t, func() bool {
		return len(ownerA.OwnedBuckets())+len(ownerB.OwnedBuckets()) == int(totalBuckets)
	}, 20*time.Second, 250*time.Millisecond)

	cancelA()

	require.Eventually(t, func() bool {
		return uint32(len(ownerB.OwnedBuckets())) == totalBuckets
	}, 30*time.Second, 250*time.Millisecond)
}

func TestDynamicShardedElector_MorePodsThanBuckets(t *testing.T) {
	clientset := newTestClientset(t)
	const totalBuckets uint32 = 2
	const numPods = 4

	electors := make([]Elector, numPods)
	for i := range numPods {
		id := fmt.Sprintf("pod-%d", i)
		electors[i] = NewDynamicShardedElector(clientset, "default", "shard-excess", id, totalBuckets)
		require.NoError(t, electors[i].Start(t.Context()))
	}

	require.Eventually(t, func() bool {
		total := 0
		for _, e := range electors {
			total += len(e.(ShardOwner).OwnedBuckets())
		}
		return total == int(totalBuckets)
	}, 20*time.Second, 250*time.Millisecond)

	leadersWithBuckets := 0
	for _, e := range electors {
		if len(e.(ShardOwner).OwnedBuckets()) > 0 {
			leadersWithBuckets++
		}
	}
	require.LessOrEqual(t, leadersWithBuckets, int(totalBuckets))
}

func TestDynamicShardedElector_InvalidConfig(t *testing.T) {
	clientset := newTestClientset(t)
	elector := NewDynamicShardedElector(clientset, "default", "shard-invalid", "pod-1", 0)
	err := elector.Start(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "totalBuckets")
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
