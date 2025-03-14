package archive

import (
	"archive/tar"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"io"
	"os"
	"path/filepath"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func DecompressZstdArchive(inputFilePath string) (string, error) {
	extractedDirectoryPath := filepath.Dir(inputFilePath)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Open decompressed file reader
	//
	tarReader, cleanup, err := OpenZstdReader(inputFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to open zstd archive: %v", err)
		logger.Logger.Error().Msg(msg)

		if cleanup != nil {
			cleanup()
		}

		return "", fmt.Errorf(msg)
	}
	defer cleanup()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Un-tar reader to extractedDirectoryPath
	//
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			var msg = fmt.Sprintf("Failed to read zstd archive: %v", err)
			logger.Logger.Error().Msg(msg)

			return "", err
		}

		targetPath := filepath.Join(extractedDirectoryPath, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.ModePerm); err != nil {
				var msg = fmt.Sprintf("Failed to create directory: %v", err)
				logger.Logger.Error().Msg(msg)

				return "", err
			}

		case tar.TypeReg:
			outFile, err := os.Create(targetPath)
			if err != nil {
				var msg = fmt.Sprintf("Failed to create file: %v", err)
				logger.Logger.Error().Msg(msg)

				return "", err
			}

			if _, err := io.Copy(outFile, tarReader); err != nil {
				err := outFile.Close()
				if err != nil {
					var msg = fmt.Sprintf("Failed to close file: %v", err)
					logger.Logger.Error().Msg(msg)

					return "", err
				}
				return "", err
			}

			if err := outFile.Close(); err != nil {
				var msg = fmt.Sprintf("Failed to close file: %v", err)
				logger.Logger.Error().Msg(msg)

				return "", err
			}
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return extractedDirectoryPath, nil
}
