package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	leaderelect "github.com/sanketsudake/k8s-operator-leader-election"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	strategy := envOrDefault("STRATEGY", "active-passive")
	namespace := envOrDefault("NAMESPACE", "default")
	leasePrefix := envOrDefault("LEASE_PREFIX", "leader-demo")
	podName := envOrDefault("POD_NAME", "unknown")
	totalBucketsStr := envOrDefault("TOTAL_BUCKETS", "3")

	slog.Info("starting leader election demo",
		"strategy", strategy,
		"namespace", namespace,
		"leasePrefix", leasePrefix,
		"podName", podName,
	)

	cfg, err := rest.InClusterConfig()
	if err != nil {
		slog.Error("failed to get in-cluster config", "error", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("failed to create clientset", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	elector, err := buildElector(strategy, clientset, namespace, leasePrefix, podName, totalBucketsStr)
	if err != nil {
		slog.Error("failed to build elector", "error", err)
		os.Exit(1)
	}

	if err := elector.Start(ctx); err != nil {
		slog.Error("failed to start elector", "error", err)
		os.Exit(1)
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		case <-ticker.C:
			logLeadershipState(elector, strategy)
		}
	}
}

func buildElector(strategy string, client kubernetes.Interface, ns, prefix, id, bucketsStr string) (leaderelect.Elector, error) {
	switch strings.ToLower(strategy) {
	case "active-passive":
		return leaderelect.NewActivePassiveElector(client, ns, prefix, id), nil

	case "sharded":
		total, err := strconv.ParseUint(bucketsStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid TOTAL_BUCKETS %q: %w", bucketsStr, err)
		}
		return leaderelect.NewDynamicShardedElector(client, ns, prefix, id, uint32(total)), nil

	case "statefulset":
		return leaderelect.NewStatefulSetElector(client, ns, prefix, id), nil

	default:
		return nil, fmt.Errorf("unknown strategy %q (valid: active-passive, sharded, statefulset)", strategy)
	}
}

func logLeadershipState(elector leaderelect.Elector, strategy string) {
	switch strings.ToLower(strategy) {
	case "sharded":
		if owner, ok := elector.(leaderelect.ShardOwner); ok {
			buckets := owner.OwnedBuckets()
			slog.Info("shard ownership", "ownedBuckets", buckets, "count", len(buckets))
		}
	default:
		slog.Info("leadership state", "isLeader", elector.IsLeader(""))
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
