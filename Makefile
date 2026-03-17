run-tests:
	@echo "Running tests..."
	@KUBEBUILDER_ASSETS="$$(go tool setup-envtest -p path use 1.30.x)"; \
	echo "KUBEBUILDER_ASSETS: $$KUBEBUILDER_ASSETS"; \
	KUBEBUILDER_ASSETS="$$KUBEBUILDER_ASSETS" go test -v ./...