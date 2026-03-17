package leaderelect

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestActivePassiveElector_EnvTest(t *testing.T) {
	// 1. Setup envtest (Spins up local API server and etcd)
	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testEnv.Stop())
	})

	clientset, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)

	// 2. Initialize the Elector
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	elector := NewActivePassiveElector(clientset, "default", "test-lock", "pod-1")

	// 3. Start election
	err = elector.Start(ctx)
	require.NoError(t, err)

	// 4. Wait for leadership to be acquired
	acquired := false
	for i := 0; i < 10; i++ {
		if elector.IsLeader("any-key") {
			acquired = true
			break
		}
		time.Sleep(1 * time.Second)
	}

	require.True(t, acquired, "expected leadership to be acquired within timeout")
}

func TestShardedElector(t *testing.T) {
	// 3 total buckets, this pod is responsible for bucket ID 1
	elector := NewShardedElector(3, 1)

	// In a real scenario, you would hash the Custom Resource namespace/name
	testKeys := []string{
		"default/pod-a", // hashes to bucket 2
		"default/pod-b", // hashes to bucket 0
		"default/pod-c", // hashes to bucket 1
	}

	// We know "default/pod-c" hashes to 1 (using FNV32a).
	require.False(t, elector.IsLeader(testKeys[0]))
	require.False(t, elector.IsLeader(testKeys[1]))
	require.True(t, elector.IsLeader(testKeys[2]))
	require.NoError(t, elector.Start(t.Context()))
}

func TestShardedElector_ZeroBuckets(t *testing.T) {
	elector := NewShardedElector(0, 0)
	require.False(t, elector.IsLeader("default/pod-any"))
}

func TestShardedElector_DeterministicForSameKey(t *testing.T) {
	key := "default/workload-a"
	electorA := NewShardedElector(3, 0)
	electorB := NewShardedElector(3, 1)

	leaderA := electorA.IsLeader(key)
	leaderB := electorB.IsLeader(key)

	require.NotEqual(t, leaderA, leaderB)
}

func TestStatefulSetElector(t *testing.T) {
	elector := NewStatefulSetElector("0")
	require.NoError(t, elector.Start(t.Context()))

	// Test Follower Pod
	require.NoError(t, os.Setenv("HOSTNAME", "my-operator-1"))
	require.False(t, elector.IsLeader(""))

	// Test Leader Pod
	require.NoError(t, os.Setenv("HOSTNAME", "my-operator-0"))
	require.True(t, elector.IsLeader(""))

	// Test Empty Hostname
	require.NoError(t, os.Setenv("HOSTNAME", ""))
	require.False(t, elector.IsLeader(""))

	// Cleanup
	require.NoError(t, os.Unsetenv("HOSTNAME"))
}
