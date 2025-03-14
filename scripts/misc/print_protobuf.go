package main

import (
	"encoding/json"
	"fmt"
	infoproto "github.com/Rom1-J/preprocessor/proto/info"
	metadataproto "github.com/Rom1-J/preprocessor/proto/metadata"
	"log"
	"os"

	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: %s <path_to_protobuf_file> <info|metadata>", os.Args[0])
	}

	filePath := os.Args[1]
	protobufType := os.Args[2]
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	var jsonData []byte

	if protobufType == "info" {
		metadataInfo := &infoproto.MetadataInfo{}
		err = proto.Unmarshal(data, metadataInfo)
		if err != nil {
			log.Fatalf("Failed to unmarshal protobuf data: %v", err)
		}

		jsonData, err = json.MarshalIndent(metadataInfo, "", "  ")
		if err != nil {
			log.Fatalf("Failed to convert to JSON: %v", err)
		}

		jsonData, err = json.MarshalIndent(metadataInfo, "", "  ")
	} else if protobufType == "metadata" {
		metadata := &metadataproto.MetadataList{}
		err = proto.Unmarshal(data, metadata)
		if err != nil {
			log.Fatalf("Failed to unmarshal protobuf data: %v", err)
		}

		jsonData, err = json.MarshalIndent(metadata, "", "  ")
		if err != nil {
			log.Fatalf("Failed to convert to JSON: %v", err)
		}

		jsonData, err = json.MarshalIndent(metadata, "", "  ")
	} else {
		log.Fatalf("Unknown protobuf type: %s", protobufType)
	}

	if err != nil {
		log.Fatalf("Failed to convert to JSON: %v", err)
	}

	fmt.Println(string(jsonData))
}
