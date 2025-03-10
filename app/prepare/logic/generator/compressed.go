package generator

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/archive"
	metadatainfoproto "github.com/Rom1-J/preprocessor/proto"
	"github.com/google/uuid"
	"github.com/segmentio/fasthash/fnv1a"
	"os"
	"path/filepath"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func ProcessCompressedFile(id string, date string, basePath string, inputFilePath string) (*metadatainfoproto.MetadataInfo, error) {
	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Get file info
	//
	relPath, err := filepath.Rel(basePath, inputFilePath)
	if err != nil {
		return nil, err
	}

	fileInfo, err := os.Stat(inputFilePath)
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	fileData, err := os.ReadFile(inputFilePath)
	if err != nil {
		return nil, err
	}
	fileSimhash := fnv1a.HashBytes64(fileData)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	var metadata = metadatainfoproto.MetadataInfo{
		Id:      id,
		Date:    date,
		Path:    []byte(strings.TrimSuffix(relPath, ".compressed")),
		Size:    uint64(fileSize),
		Simhash: fileSimhash,
	}

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Extracting archive
	//
	extractedDirectoryPath, err := archive.DecompressZstdArchive(inputFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to decompress %s for metadata: %v", inputFilePath, err)
		logger.Logger.Error().Msg(msg)

		return nil, fmt.Errorf(msg)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Delete old archive file
	//
	err = os.Remove(inputFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to remove %s: %v", inputFilePath, err)
		logger.Logger.Warn().Msg(msg)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Get archive entries
	//
	entries, err := os.ReadDir(extractedDirectoryPath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to read directory %s: %v", extractedDirectoryPath, err)
		logger.Logger.Warn().Msg(msg)

		return &metadata, nil
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Iter archive entries
	//
	for _, entry := range entries {
		path := filepath.Join(extractedDirectoryPath, entry.Name())

		var entryMetadata *metadatainfoproto.MetadataInfo

		if entry.IsDir() {
			entryMetadata, err = GenerateForDirectory(uuid.New().String(), date, extractedDirectoryPath, path)
		} else {
			entryMetadata, err = GenerateForFile(uuid.New().String(), date, extractedDirectoryPath, path)
		}

		if err != nil {
			var msg = fmt.Sprintf("Failed to get metadata for %s: %v", path, err)
			logger.Logger.Error().Msg(msg)
			continue
		}

		metadata.Children = append(metadata.Children, entryMetadata)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return &metadata, nil
}
