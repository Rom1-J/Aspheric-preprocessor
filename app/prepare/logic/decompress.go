package logic

import (
	"archive/tar"
	"bytes"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	metadatainfoproto "github.com/Rom1-J/preprocessor/proto"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/segmentio/fasthash/fnv1a"
	"io"
	"os"
	"path/filepath"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func ExtractZstdArchive(date string, inputFilePath string, outputDirectory string) (*metadatainfoproto.MetadataInfo, error) {
	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Retrieving file descriptor
	//
	compressedData, err := os.ReadFile(inputFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to read zstd archive: %v", err)
		logger.Logger.Error().Msg(msg)

		return nil, err
	}

	extractedDirectoryPath := filepath.Join(
		outputDirectory,
		strings.TrimSuffix(filepath.Base(inputFilePath), ".compressed")+".extracted",
	)
	err = os.MkdirAll(extractedDirectoryPath, os.ModePerm)
	if err != nil {
		var msg = fmt.Sprintf("Failed to create extracted directory: %v", err)
		logger.Logger.Error().Msg(msg)

		return nil, err
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Open decompressed file reader
	//
	reader, err := zstd.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		var msg = fmt.Sprintf("Failed to read zstd archive: %v", err)
		logger.Logger.Error().Msg(msg)

		return nil, err
	}
	defer reader.Close()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Un-tar reader to extractedDirectoryPath
	//
	tarReader := tar.NewReader(reader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			var msg = fmt.Sprintf("Failed to read zstd archive: %v", err)
			logger.Logger.Error().Msg(msg)

			return nil, err
		}

		targetPath := filepath.Join(extractedDirectoryPath, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.ModePerm); err != nil {
				var msg = fmt.Sprintf("Failed to create directory: %v", err)
				logger.Logger.Error().Msg(msg)

				return nil, err
			}

		case tar.TypeReg:
			outFile, err := os.Create(targetPath)
			if err != nil {
				var msg = fmt.Sprintf("Failed to create file: %v", err)
				logger.Logger.Error().Msg(msg)

				return nil, err
			}

			if _, err := io.Copy(outFile, tarReader); err != nil {
				err := outFile.Close()
				if err != nil {
					var msg = fmt.Sprintf("Failed to close file: %v", err)
					logger.Logger.Error().Msg(msg)

					return nil, err
				}
				return nil, err
			}

			if err := outFile.Close(); err != nil {
				var msg = fmt.Sprintf("Failed to close file: %v", err)
				logger.Logger.Error().Msg(msg)

				return nil, err
			}
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Process extracted files
	//
	children, err := ProcessExtractedFiles(date, extractedDirectoryPath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to process extracted files: %v", err)
		logger.Logger.Error().Msg(msg)

		return nil, err
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Compress back extracted files
	//
	if _, err := CompressZstdArchive(extractedDirectoryPath); err != nil {
		var msg = fmt.Sprintf("Failed to compress extracted files: %v", err)
		logger.Logger.Error().Msg(msg)

		return nil, err
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Delete extracted data
	//
	if err := os.RemoveAll(extractedDirectoryPath); err != nil {
		var msg = fmt.Sprintf("Failed to remove extracted files: %v", err)
		logger.Logger.Error().Msg(msg)

		return nil, err
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return &metadatainfoproto.MetadataInfo{
		Id:       uuid.New().String(),
		Date:     date,
		Path:     filepath.Base(inputFilePath),
		Size:     uint64(len(compressedData)),
		Simhash:  fnv1a.HashBytes64(compressedData),
		Children: children,
	}, nil
}
