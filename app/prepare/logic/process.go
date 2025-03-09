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

func ProcessSingleFile(date string, basePath string, filePath string) (*metadatainfoproto.MetadataInfo, error) {
	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Getting file info
	//
	relPath, err := filepath.Rel(basePath, filePath)
	if err != nil {
		return nil, err
	}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Simhashing file
	//
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	metadata := &metadatainfoproto.MetadataInfo{
		Id:      uuid.New().String(),
		Date:    date,
		Path:    relPath,
		Size:    uint64(fileSize),
		Simhash: fnv1a.HashBytes64(fileData),
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return metadata, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func ProcessExtractedFiles(date string, basePath string) ([]*metadatainfoproto.MetadataInfo, error) {
	var children []*metadatainfoproto.MetadataInfo

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Iterate files in basePath
	//
	err := filepath.WalkDir(basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Abort when top root
		//
		relPath, _ := filepath.Rel(basePath, path)
		if relPath == "." {
			return nil
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		info, err := d.Info()
		if err != nil {
			logger.Logger.Error().Msgf("Failed to get info for %s: %v", path, err)
			return nil
		}

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Chunkify large files
		//
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return nil
		}

		if !d.IsDir() && utils.CanBeChunks(path) {
			chunkedDirPath, err := Chunkify(path)
			if err != nil {
				var msg = fmt.Sprintf("Failed to chunkify file %s: %v", path, err)
				logger.Logger.Error().Msg(msg)

				return nil
			}
			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Delete old file
			//
			if err := os.Remove(path); err != nil {
				var msg = fmt.Sprintf("Failed to remove %s: %v", path, err)
				logger.Logger.Warn().Msg(msg)
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

			return filepath.WalkDir(chunkedDirPath, func(chunkPath string, chunkD fs.DirEntry, chunkErr error) error {
				if chunkErr != nil {
					return chunkErr
				}

				chunkInfo, err := chunkD.Info()
				if err != nil {
					var msg = fmt.Sprintf("Failed to get info for chunked file %s: %v", chunkPath, err)
					logger.Logger.Error().Msg(msg)

					return nil
				}

				if !chunkD.IsDir() {
					chunkData, err := os.ReadFile(chunkPath)
					if err != nil {
						var msg = fmt.Sprintf("Failed to read chunked file %s: %v", chunkPath, err)
						logger.Logger.Error().Msg(msg)

						return nil
					}

					rel, _ := filepath.Rel(basePath, chunkPath)
					children = append(children, &metadatainfoproto.MetadataInfo{
						Id:      uuid.New().String(),
						Date:    date,
						Path:    rel,
						Size:    uint64(chunkInfo.Size()),
						Simhash: fnv1a.HashBytes64(chunkData),
					})
				}

				return nil
			})
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Generate metadata info
		//
		metadata := &metadatainfoproto.MetadataInfo{
			Id:   uuid.New().String(),
			Date: date,
			Path: relPath,
			Size: uint64(info.Size()),
		}

		if info.IsDir() {
			metadata.Children, err = ProcessExtractedFiles(date, path)
			if err != nil {
				var msg = fmt.Sprintf("Failed to process extracted files in %s: %v", path, err)
				logger.Logger.Error().Msg(msg)

				return nil
			}
		} else {
			fileData, err := os.ReadFile(path)
			if err != nil {
				var msg = fmt.Sprintf("Failed to read file %s: %v", path, err)
				logger.Logger.Error().Msg(msg)

				return nil
			}
			metadata.Simhash = fnv1a.HashBytes64(fileData)
		}

		children = append(children, metadata)
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		return nil
	})
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return children, err
}
