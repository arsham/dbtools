.PHONY: test
test:
	@echo "running tests on $(run). waiting for changes..."
	@-zsh -c "go test ./...; repeat 100 printf '#'; echo"
	@reflex -d none -r "(\.go$$)|(go.mod)" -- zsh -c "go test ./...; repeat 100 printf '#'"

.PHONY: test_race
test_race:
	@echo "running tests on $(run). waiting for changes..."
	@-zsh -c "go test -race ./...; repeat 100 printf '#'; echo"
	@reflex -d none -r "(\.go$$)|(go.mod)" -- zsh -c "go test -race ./...; repeat 100 printf '#'"

.PHONY: third-party
third-party:
	@go get -u github.com/cespare/reflex

.PHONY: clean
clean:
	go clean -cache -testcache
