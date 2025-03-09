package logic

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

func generateForFile(id string, date string, basePath string, inputFilePath string) (*metadatainfoproto.MetadataInfo, error) {
	logger.Logger.Trace().Msgf("Generating metadata for file %s from %s", inputFilePath, basePath)
	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Getting file info
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
		chunkedDirPath, err := Chunkify(inputFilePath)
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
		// Getting metadata for new files
		//
		if err := filepath.WalkDir(chunkedDirPath, func(path string, d fs.DirEntry, err error) error {
			if _, err := os.Stat(path); os.IsNotExist(err) || d.IsDir() {
				return nil
			}

			partMetadata, err := generateForFile(uuid.New().String(), date, basePath, path)
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

func ProcessTextFile(id string, date string, basePath string, inputFilePath string) (*metadatainfoproto.MetadataInfo, error) {
	metadata, err := generateForFile(id, date, basePath, inputFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to get metadata for %s: %v", inputFilePath, err)
		logger.Logger.Error().Msg(msg)

		return nil, fmt.Errorf(msg)
	}

	return metadata, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func ProcessCompressedFile(id string, date string, basePath string, inputFilePath string) (*metadatainfoproto.MetadataInfo, error) {
	return nil, nil

}
