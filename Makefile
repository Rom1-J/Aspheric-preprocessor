EXTENSION ?=
DIST_DIR ?= dist/
GOOS ?= linux
ARCH ?= $(shell uname -m)
BUILDINFOSDET ?=

SOFT_NAME     := preprocessor
SOFT_VERSION  := $(shell (git describe --tags $(git rev-list --tags --max-count=1)) || echo "0.0.0")
VERSION_PKG   := $(shell echo $(SOFT_VERSION) | sed 's/^v//g')
ARCH          := x86_64
URL           := https://github.com/Rom1-J/preprocessor
DESCRIPTION   := Convert text, csv, sql, .. files into ndjson.
BUILDINFOS    := ($(shell date +%FT%T%z)$(BUILDINFOSDET))
LDFLAGS       := '-X main.version=$(SOFT_VERSION) -X main.buildinfos=$(BUILDINFOS)'

OUTPUT_SOFT   := $(DIST_DIR)$(SOFT_NAME)-$(SOFT_VERSION)-$(GOOS)-$(ARCH)$(EXTENSION)

.PHONY: vet
vet:
	go vet main.go

.PHONY: prepare
prepare:
	mkdir -p $(DIST_DIR)

.PHONY: clean
clean:
	rm -rf $(DIST_DIR)

.PHONY: build
build: prepare
	go build -ldflags $(LDFLAGS) -o $(OUTPUT_SOFT)

.PHONY: neo4j
neo4j:
	docker run \
      -p7474:7474 -p7687:7687 \
      --env=NEO4J_AUTH=neo4j/secretgraph \
      --env=NEO4J_server_memory_heap_max__size=48G \
      --env=NEO4J_server_memory_heap_initial__size=48G \
      --env=NEO4J_server_memory_pagecache_size=12G \
      --env=NEO4J_server_threads_worker__count=32 \
      --env=NEO4J_server_jvm_additional=-XX:+UseG1GC \
      neo4j:latest
