EXTENSION ?=
DIST_DIR ?= dist/
GOOS ?= linux
ARCH ?= $(shell uname -m)
BUILDINFOSDET ?=

SOFT_NAME    := preprocessor
SOFT_VERSION := $(shell git describe --tags $(git rev-list --tags --max-count=1))
VERSION_PKG   := $(shell echo $(SOFT_VERSION) | sed 's/^v//g')
ARCH          := x86_64
URL           := https://github.com/Rom1-J/preprocessor
DESCRIPTION   := Convert text, csv, sql, .. files into ndjson.
BUILDINFOS    :=  ($(shell date +%FT%T%z)$(BUILDINFOSDET))
LDFLAGS       := '-X main.version=$(SOFT_VERSION) -X main.buildinfos=$(BUILDINFOS)'

OUTPUT_SOFT := $(DIST_DIR)$(SOFT_NAME)-$(SOFT_VERSION)-$(GOOS)-$(ARCH)$(EXTENSION)

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