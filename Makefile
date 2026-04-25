KIND_CLUSTER  ?= leader-demo
IMAGE         ?= leader-demo:local

run-tests:
	@echo "Running tests..."
	@KUBEBUILDER_ASSETS="$$(go tool setup-envtest -p path use 1.30.x)"; \
	echo "KUBEBUILDER_ASSETS: $$KUBEBUILDER_ASSETS"; \
	KUBEBUILDER_ASSETS="$$KUBEBUILDER_ASSETS" go test -v ./...

.PHONY: docker-build
docker-build:
	docker build -t $(IMAGE) .

.PHONY: kind-create
kind-create:
	kind create cluster --name $(KIND_CLUSTER)

.PHONY: kind-load
kind-load: docker-build
	kind load docker-image $(IMAGE) --name $(KIND_CLUSTER)

.PHONY: kind-deploy-rbac
kind-deploy-rbac:
	kubectl apply -f deploy/rbac.yaml

.PHONY: kind-cleanup-rbac
kind-cleanup-rbac:
	kubectl delete -f deploy/rbac.yaml

.PHONY: kind-deploy-active-passive
kind-deploy-active-passive: kind-deploy-rbac
	kubectl apply -f deploy/active-passive.yaml

.PHONY: kind-cleanup-active-passive
kind-cleanup-active-passive:
	kubectl delete -f deploy/active-passive.yaml

.PHONY: kind-deploy-sharded
kind-deploy-sharded: kind-deploy-rbac
	kubectl apply -f deploy/sharded.yaml

.PHONY: kind-cleanup-sharded
kind-cleanup-sharded:
	kubectl delete -f deploy/sharded.yaml

.PHONY: kind-deploy-statefulset
kind-deploy-statefulset: kind-deploy-rbac
	kubectl apply -f deploy/statefulset.yaml

.PHONY: kind-cleanup-statefulset
kind-cleanup-statefulset:
	kubectl delete -f deploy/statefulset.yaml

.PHONY: kind-deploy-all
kind-deploy-all: kind-deploy-active-passive kind-deploy-sharded kind-deploy-statefulset

.PHONY: kind-logs
kind-logs:
	kubectl logs -l app=leader-demo -f --max-log-requests=10

.PHONY: kind-cleanup
kind-cleanup: kind-cleanup-active-passive kind-cleanup-sharded kind-cleanup-statefulset
	$(MAKE) kind-cleanup-rbac
	kind delete cluster --name $(KIND_CLUSTER)