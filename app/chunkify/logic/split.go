package logic

import (
	"bufio"
	"fmt"
	"github.com/Rom1-J/preprocessor/app/chunkify/structs"
	"github.com/Rom1-J/preprocessor/constants"
	"github.com/Rom1-J/preprocessor/logger"
	"os"
	"path/filepath"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SplitFile(inputFilePath string, outputDirectoryPath string) (structs.SplitFileStruct, error) {
	logger.Logger.Debug().Msgf("SplitFile starting on: %s", inputFilePath)

	var (
		currentSize int64
		fileIndex   int
		outputFile  *os.File
		writer      *bufio.Writer

		lines int

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

		return structs.SplitFileStruct{}, err
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

		return structs.SplitFileStruct{}, err
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Split files
	//
	for {
		line, err := reader.ReadString('\n')
		lines += 1
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			var msg = fmt.Sprintf("Error reading file: %v", err)
			logger.Logger.Error().Msgf(msg)

			return structs.SplitFileStruct{}, err
		}

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Save chunk
		//
		if outputFile == nil || currentSize+int64(len(line)) > constants.ChunkSize {
			if writer != nil {
				if err := writer.Flush(); err != nil {
					return structs.SplitFileStruct{}, err
				}
			}
			if outputFile != nil {
				if err := outputFile.Close(); err != nil {
					return structs.SplitFileStruct{}, err
				}
			}

			outputFileName := filepath.Join(outputDirectoryPath, fmt.Sprintf("%s.part%d", baseName, fileIndex))
			outputFile, err = os.Create(outputFileName)
			if err != nil {
				var msg = fmt.Sprintf("Failed to create output file: %v", err)
				logger.Logger.Error().Msgf(msg)

				return structs.SplitFileStruct{}, err
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

			return structs.SplitFileStruct{}, err
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
			return structs.SplitFileStruct{}, err
		}
	}
	if outputFile != nil {
		if err := outputFile.Close(); err != nil {
			return structs.SplitFileStruct{}, err
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	logger.Logger.Info().Msgf("File split into %d chunks.", fileIndex)
	return structs.SplitFileStruct{
		Lines: lines,
		Parts: fileIndex,
	}, nil
}
