package logic

import (
	"bufio"
	"fmt"
	"github.com/Rom1-J/preprocessor/app/chunkify/structs"
	"github.com/Rom1-J/preprocessor/constants"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	"github.com/jedib0t/go-pretty/v6/progress"
	"os"
	"path/filepath"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SplitFile(globalProgress prog.ProgressOptsStruct, inputFilePath string, outputDirectoryPath string) (structs.SplitFileStruct, error) {
	logger.Logger.Debug().Msgf("SplitFile starting on: %s", inputFilePath)

	var (
		fileSize    int64
		currentSize int64
		overallSize int64
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

	fs, err := file.Stat()
	if err != nil {
		var msg = fmt.Sprintf("Failed to stat input file: %v", err)
		logger.Logger.Error().Msgf(msg)

		return structs.SplitFileStruct{}, err
	}

	fileSize = fs.Size()

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

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initialize tracker
	//
	tracker := progress.Tracker{
		Message: "Processing file " + filepath.Base(inputFilePath),
		Total:   fileSize,
		Units:   progress.UnitsBytes,
	}
	globalProgress.Pw.AppendTracker(&tracker)
	globalProgress.GlobalTracker.UpdateTotal(globalProgress.GlobalTracker.Total + 1)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

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

			logger.Logger.Debug().Msgf("Creating new chunk: %s", outputFileName)

			overallSize += currentSize
			tracker.SetValue(overallSize)

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

	logger.Logger.Info().Msgf("File '%s' split into %d chunks.", inputFilePath, fileIndex)

	tracker.MarkAsDone()

	return structs.SplitFileStruct{
		Lines: lines,
		Parts: fileIndex,
	}, nil
}
