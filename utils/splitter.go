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
	logger.Logger.Debug().Msgf("SplitFile starting on: %s", inputFilePath)

	var (
		currentSize int64
		fileIndex   int
		outputFile  *os.File
		writer      *bufio.Writer

		err error
	)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initializing file reader
	//
	file, err := os.Open(inputFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to open input file: %v", err)
		logger.Logger.Error().Msgf(msg)

		return err
	}

	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			logger.Logger.Error().Msgf("Failed to close input file: %v", err)
		}
	}(file)

	reader := bufio.NewReader(file)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Retrieve file basename
	//
	baseName := filepath.Base(inputFilePath)
	if err = os.MkdirAll(outputDirectoryPath, 0755); err != nil {
		var msg = fmt.Sprintf("Failed to open input file: %v", err)
		logger.Logger.Error().Msgf(msg)

		return err
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Split files
	//
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			var msg = fmt.Sprintf("Error reading file: %v", err)
			logger.Logger.Error().Msgf(msg)

			return err
		}

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Save chunk
		//
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
				var msg = fmt.Sprintf("Failed to create output file: %v", err)
				logger.Logger.Error().Msgf(msg)

				return err
			}
			writer = bufio.NewWriter(outputFile)

			logger.Logger.Info().Msgf("Creating new chunk: %s", outputFileName)

			currentSize = 0
			fileIndex++
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		n, writeErr := writer.WriteString(line)
		if writeErr != nil {
			var msg = fmt.Sprintf("Error writing to file: %v", writeErr)
			logger.Logger.Error().Msgf(msg)

			return err
		}

		currentSize += int64(n)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Close handlers
	//
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
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	logger.Logger.Info().Msgf("File split into %d chunks.", fileIndex)
	return nil
}
