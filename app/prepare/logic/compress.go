package logic

import (
	"archive/tar"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/klauspost/compress/zstd"
	"io"
	"os"
	"path/filepath"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func CompressZstdArchive(inputDirectoryPath string) (string, error) {
	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Generate output file
	//
	outputFilePath := filepath.Join(filepath.Dir(inputDirectoryPath), filepath.Base(inputDirectoryPath)+".compressed")

	outFile, err := os.Create(outputFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to create output file: %w", err)
	}
	defer func(outFile *os.File) {
		err := outFile.Close()
		if err != nil {
			var msg = fmt.Sprintf("Failed to close output file: %v", err)
			logger.Logger.Error().Msg(msg)
		}
	}(outFile)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Create zstd writer
	//
	zstdWriter, err := zstd.NewWriter(outFile)
	if err != nil {
		return "", fmt.Errorf("failed to create zstd writer: %w", err)
	}
	defer func(zstdWriter *zstd.Encoder) {
		err := zstdWriter.Close()
		if err != nil {
			var msg = fmt.Sprintf("Failed to close zstd writer: %v", err)
			logger.Logger.Error().Msg(msg)
		}
	}(zstdWriter)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Create tar writer
	//
	tarWriter := tar.NewWriter(zstdWriter)
	defer func(tarWriter *tar.Writer) {
		err := tarWriter.Close()
		if err != nil {
			var msg = fmt.Sprintf("Failed to close tar writer: %v", err)
			logger.Logger.Error().Msg(msg)
		}
	}(tarWriter)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Tar & compress files
	//
	err = filepath.Walk(inputDirectoryPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(inputDirectoryPath, filePath)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, filePath)
		if err != nil {
			return err
		}
		header.Name = relPath

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		if !info.IsDir() {
			file, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					var msg = fmt.Sprintf("Failed to close tar writer: %v", err)
					logger.Logger.Error().Msg(msg)
				}
			}(file)

			_, err = io.Copy(tarWriter, file)
			if err != nil {
				return err
			}
		}

		return nil
	})
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	if err != nil {
		return "", fmt.Errorf("error while archiving: %w", err)
	}

	return outputFilePath, nil
}
