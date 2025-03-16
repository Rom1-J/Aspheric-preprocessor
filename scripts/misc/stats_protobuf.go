package main

import (
	"fmt"
	metadataproto "github.com/Rom1-J/preprocessor/proto/metadata"
	"log"
	"os"

	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <path_to_protobuf_file>", os.Args[0])
	}

	filePath := os.Args[1]
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	metadata := &metadataproto.MetadataList{}
	err = proto.Unmarshal(data, metadata)
	if err != nil {
		log.Fatalf("Failed to unmarshal protobuf data: %v", err)
	}

	var (
		files   int
		emails  int
		domains int
		ips     int
	)

	for _, item := range metadata.Items {
		files++
		emails += len(item.Emails)
		domains += len(item.Domains)
		ips += len(item.Ips)
	}

	fmt.Println(fmt.Sprintf("Files: %d | emails: %d | domains: %d | ips: %d", files, emails, domains, ips))
}
