package generator

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/utils"
	infoproto "github.com/Rom1-J/preprocessor/proto/info"
	"github.com/google/uuid"
	"github.com/segmentio/fasthash/fnv1a"
	ucli "github.com/urfave/cli/v3"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func getBucketType(command *ucli.Command) infoproto.Bucket {
	switch strings.ToLower(command.String("bucket")) {
	case "leaks.logs":
		return infoproto.Bucket_LEAKS_LOGS
	case "leaks.databases":
		return infoproto.Bucket_LEAKS_DATABASES
	case "combinations":
		return infoproto.Bucket_COMBINATIONS
	case "pastes":
		return infoproto.Bucket_PASTES
	default:
		return infoproto.Bucket_DUMPSTER
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GenerateForFile(id string, command *ucli.Command, inputFilePath string) (*infoproto.MetadataInfo, error) {
	logger.Logger.Trace().Msgf("Generating metadata info for file %s", inputFilePath)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Get file info
	//
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

	var metadata = infoproto.MetadataInfo{
		Id:      id,
		Bucket:  getBucketType(command),
		Date:    command.String("date"),
		Path:    []byte(filepath.Base(inputFilePath)),
		Size:    uint64(fileSize),
		Simhash: fileSimhash,
	}

	if utils.IsChunkable(inputFilePath) {
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
		metadata.Path = []byte(filepath.Base(chunkedDirPath))
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

			partMetadata, err := GenerateForFile(uuid.New().String(), command, path)
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

func GenerateForDirectory(id string, command *ucli.Command, inputDirectoryPath string) (*infoproto.MetadataInfo, error) {
	logger.Logger.Trace().Msgf("Generating metadata info for directory %s", inputDirectoryPath)

	var metadata = infoproto.MetadataInfo{
		Id:   id,
		Date: command.String("date"),
		Path: []byte(filepath.Base(inputDirectoryPath)),
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

		var entryMetadata *infoproto.MetadataInfo

		if entry.IsDir() {
			entryMetadata, err = GenerateForDirectory(uuid.New().String(), command, path)
		} else {
			entryMetadata, err = GenerateForFile(uuid.New().String(), command, path)
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
