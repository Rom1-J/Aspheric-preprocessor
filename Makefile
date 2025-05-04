EXTENSION ?=
DIST_DIR ?= dist/
GOOS ?= linux
ARCH ?= $(shell uname -m)
BUILDINFOSDET ?=

SOFT_NAME     := preprocessor
SOFT_VERSION  := $(shell (git describe --tags $(git rev-list --tags --max-count=1)) || echo "0.0.0")
VERSION_PKG   := $(shell echo $(SOFT_VERSION) | sed 's/^v//g')
ARCH          := x86_64
URL           := https://github.com/Rom1-J/Aspheric-preprocessor
DESCRIPTION   := Convert text, csv, sql, .. files into csv.
BUILDINFOS    := ($(shell date +%FT%T%z)$(BUILDINFOSDET))
LDFLAGS       := -X main.version=$(SOFT_VERSION) -X main.buildinfos=$(BUILDINFOS)

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
	go build -ldflags "$(LDFLAGS)" -o "$(OUTPUT_SOFT)"

	go build -ldflags "-s -w -buildid=" -gcflags "-l -B" -trimpath -o "$(OUTPUT_SOFT).smol"

.PHONY: protoc
protoc:
	protoc \
      --go_out=./ \
      --go_opt=paths=source_relative \
      --go_opt=Mproto/info/info.proto=proto/info/info.proto \
      --python_out=./ \
      ./proto/info/info.proto

	protoc \
	  --go_out=./ \
	  --go_opt=paths=source_relative \
	  --go_opt=Mproto/metadata/metadata.proto=proto/metadata/metadata.proto \
	  --python_out=./ \
	  ./proto/metadata/metadata.proto
