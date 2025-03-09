package main

import (
	"encoding/json"
	"fmt"
	metadatainfoproto "github.com/Rom1-J/preprocessor/proto"
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

	metadata := &metadatainfoproto.MetadataInfo{}
	err = proto.Unmarshal(data, metadata)
	if err != nil {
		log.Fatalf("Failed to unmarshal protobuf data: %v", err)
	}

	jsonData, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		log.Fatalf("Failed to convert to JSON: %v", err)
	}

	fmt.Println(string(jsonData))
}
