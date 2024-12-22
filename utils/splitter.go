package utils

import (
	"bufio"
	"fmt"
	"github.com/Rom1-J/preprocessor/constants"
	"github.com/Rom1-J/preprocessor/logger"
	"os"
	"path/filepath"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SplitFile(inputFilePath string, outputDirectoryPath string) error {
	file, err := os.Open(inputFilePath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			logger.Logger.Error().Msgf("failed to close input file: %v", err)
		}
	}(file)

	baseName := filepath.Base(inputFilePath)
	if err = os.MkdirAll(outputDirectoryPath, 0755); err != nil {
		return nil
	}

	reader := bufio.NewReader(file)

	var (
		currentSize int64
		fileIndex   int
		outputFile  *os.File
		writer      *bufio.Writer
	)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("error reading file: %v", err)
		}

		if outputFile == nil || currentSize+int64(len(line)) > constants.ChunkSize {
			if writer != nil {
				if err := writer.Flush(); err != nil {
					return err
				}
			}
			if outputFile != nil {
				if err := outputFile.Close(); err != nil {
					return err
				}
			}

			outputFileName := filepath.Join(outputDirectoryPath, fmt.Sprintf("%s.part%d", baseName, fileIndex))
			outputFile, err = os.Create(outputFileName)
			if err != nil {
				return fmt.Errorf("failed to create output file: %v", err)
			}
			writer = bufio.NewWriter(outputFile)

			logger.Logger.Info().Msgf("Creating new chunk: %s", outputFileName)

			currentSize = 0
			fileIndex++
		}

		n, writeErr := writer.WriteString(line)
		if writeErr != nil {
			return fmt.Errorf("error writing to file: %v", writeErr)
		}

		currentSize += int64(n)
	}

	if writer != nil {
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	if outputFile != nil {
		if err := outputFile.Close(); err != nil {
			return err
		}
	}

	logger.Logger.Info().Msgf("File split into %d chunks.", fileIndex)
	return nil
}
