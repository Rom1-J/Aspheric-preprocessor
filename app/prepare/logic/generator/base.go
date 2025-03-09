package generator

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/utils"
	metadatainfoproto "github.com/Rom1-J/preprocessor/proto"
	"github.com/google/uuid"
	"github.com/segmentio/fasthash/fnv1a"
	"io/fs"
	"os"
	"path/filepath"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GenerateForFile(id string, date string, basePath string, inputFilePath string) (*metadatainfoproto.MetadataInfo, error) {
	logger.Logger.Trace().Msgf("Generating metadata for file %s from %s", inputFilePath, basePath)
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
		Path:    relPath,
		Size:    uint64(fileSize),
		Simhash: fileSimhash,
	}

	if utils.CanBeChunks(inputFilePath) {
		logger.Logger.Trace().Msgf("File %s too big, chunking it", inputFilePath)

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Chunkify file
		//
		chunkedDirPath, err := utils.Chunkify(inputFilePath)
		if err != nil {
			var msg = fmt.Sprintf("Failed to chunkify file %s: %v", inputFilePath, err)
			logger.Logger.Error().Msg(msg)

			return nil, fmt.Errorf(msg)
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Update metadata Path as .chunked
		//
		relPath, err := filepath.Rel(basePath, chunkedDirPath)
		if err != nil {
			return nil, err
		}
		metadata.Path = relPath
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Delete old file
		//
		if err := os.Remove(inputFilePath); err != nil {
			var msg = fmt.Sprintf("Failed to remove %s: %v", inputFilePath, err)
			logger.Logger.Warn().Msg(msg)
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Get metadata for new files
		//
		if err := filepath.WalkDir(chunkedDirPath, func(path string, d fs.DirEntry, err error) error {
			if _, err := os.Stat(path); os.IsNotExist(err) || d.IsDir() {
				return nil
			}

			partMetadata, err := GenerateForFile(uuid.New().String(), date, basePath, path)
			if err != nil {
				return err
			}

			metadata.Children = append(metadata.Children, partMetadata)

			return nil
		}); err != nil {
			var msg = fmt.Sprintf("Failed to walk %s: %v", chunkedDirPath, err)
			logger.Logger.Warn().Msg(msg)
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	}

	return &metadata, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GenerateForDirectory(id string, date string, basePath string, inputDirectoryPath string) (*metadatainfoproto.MetadataInfo, error) {
	logger.Logger.Trace().Msgf("Generating metadata for directory %s from %s", inputDirectoryPath, basePath)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Get dir info
	//
	relPath, err := filepath.Rel(basePath, inputDirectoryPath)
	if err != nil {
		return nil, err
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	var metadata = metadatainfoproto.MetadataInfo{
		Id:   id,
		Date: date,
		Path: relPath,
	}

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Get directory entries
	//
	entries, err := os.ReadDir(inputDirectoryPath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to read directory %s: %v", inputDirectoryPath, err)
		logger.Logger.Warn().Msg(msg)

		return nil, nil
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Iter directory entries
	//
	for _, entry := range entries {
		path := filepath.Join(inputDirectoryPath, entry.Name())

		var entryMetadata *metadatainfoproto.MetadataInfo

		if entry.IsDir() {
			entryMetadata, err = GenerateForDirectory(uuid.New().String(), date, inputDirectoryPath, path)
		} else {
			entryMetadata, err = GenerateForFile(uuid.New().String(), date, inputDirectoryPath, path)
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
