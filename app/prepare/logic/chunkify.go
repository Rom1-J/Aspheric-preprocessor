package logic

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

func Chunkify(filePath string) (string, error) {
	logger.Logger.Trace().Msgf("Start file splitting on: %s", filePath)
	var (
		currentSize int64
		overallSize int64
		fileIndex   int
		outputFile  *os.File
		writer      *bufio.Writer

		err error
	)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initializing file reader
	//
	file, err := os.Open(filePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to open input file: %v", err)
		logger.Logger.Error().Msg(msg)

		return "", err
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
	baseName := filepath.Base(filePath)

	parentDir := filepath.Dir(filePath)
	outputDirectory := filepath.Join(parentDir, baseName+".chunked")

	if err = os.MkdirAll(outputDirectory, 0755); err != nil {
		var msg = fmt.Sprintf("Failed to create chunked directory: %v", err)
		logger.Logger.Error().Msg(msg)

		return "", err
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
			logger.Logger.Error().Msg(msg)

			return "", err
		}

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Save chunk
		//
		if outputFile == nil || currentSize+int64(len(line)) > constants.ChunkSize {
			if writer != nil {
				if err := writer.Flush(); err != nil {
					var msg = fmt.Sprintf("Failed to flush chunked file: %v", err)
					logger.Logger.Error().Msg(msg)

					return "", err
				}
			}
			if outputFile != nil {
				if err := outputFile.Close(); err != nil {
					var msg = fmt.Sprintf("Failed to close output file: %v", err)
					logger.Logger.Error().Msg(msg)

					return "", err
				}
			}

			outputFileName := filepath.Join(outputDirectory, fmt.Sprintf("%s.part%d", baseName, fileIndex))
			outputFile, err = os.Create(outputFileName)
			if err != nil {
				var msg = fmt.Sprintf("Failed to create output file: %v", err)
				logger.Logger.Error().Msg(msg)

				return "", err
			}
			writer = bufio.NewWriter(outputFile)

			logger.Logger.Trace().Msgf("Creating new chunk: %s", outputFileName)

			overallSize += currentSize

			currentSize = 0
			fileIndex++
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		if writer != nil {
			n, writeErr := writer.WriteString(line)
			if writeErr != nil {
				var msg = fmt.Sprintf("Error writing to file: %v", writeErr)
				logger.Logger.Error().Msg(msg)

				return "", err
			}

			currentSize += int64(n)
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Close handlers
	//
	if writer != nil {
		if err := writer.Flush(); err != nil {
			var msg = fmt.Sprintf("Error writing to file: %v", err)
			logger.Logger.Error().Msg(msg)

			return "", err
		}
	}
	if outputFile != nil {
		if err := outputFile.Close(); err != nil {
			var msg = fmt.Sprintf("Failed to close output file: %v", err)
			logger.Logger.Error().Msg(msg)

			return "", err
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	logger.Logger.Info().Msgf("File '%s' split into %d chunks.", filePath, fileIndex)

	return outputDirectory, nil
}
