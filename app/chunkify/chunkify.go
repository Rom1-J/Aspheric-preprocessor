package chunkify

import (
	"context"
	"encoding/csv"
	"github.com/Rom1-J/preprocessor/app/chunkify/logic"
	"github.com/Rom1-J/preprocessor/app/chunkify/structs"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	"github.com/Rom1-J/preprocessor/pkg/utils"
	"github.com/google/uuid"
	ucli "github.com/urfave/cli/v3"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func Action(ctx context.Context, command *ucli.Command) error {
	logger.SetLoggerLevel(command)

	var (
		inputList []string

		err error
	)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Retrieving input descriptors
	// todo: dedupe code logic
	//
	inputFiles := command.StringSlice("input")
	inputDirectories := command.StringSlice("directory")

	inputList = append(inputList, inputFiles...)

	for _, directory := range inputDirectories {
		if err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				inputList = append(inputList, path)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	logger.Logger.Trace().Msgf("Input files: %v", inputList)

	logger.Logger.Info().Msgf("Chunkifing %d files", len(inputList))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initialize progress bar
	//
	globalProgress := prog.New("Files processed", len(inputList))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Chunkify files
	//
	var wg sync.WaitGroup
	maxThreads := int(command.Int("threads"))
	semaphore := make(chan struct{}, maxThreads)

	for _, inputFile := range inputList {
		logger.Logger.Trace().Msgf("Locking slot for: %s", inputFile)
		semaphore <- struct{}{}
		wg.Add(1)

		go func(filePath string) {
			defer func() {
				logger.Logger.Trace().Msgf("Releasing slot for: %s", filePath)
				<-semaphore
				wg.Done()

				globalProgress.GlobalTracker.Increment(1)
			}()

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Retrieving output descriptor
			//
			outputDirectoryPath := filepath.Join(command.String("output"), uuid.New().String())
			metadataInfoFilePath := filepath.Join(outputDirectoryPath, "_info.csv")

			absoluteOutputDirectoryPath, err := filepath.Abs(outputDirectoryPath)
			if err != nil {
				logger.Logger.Error().Msgf("Error getting absolute output path: %v", err)

				return
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

			stats, err := logic.SplitFile(
				globalProgress,
				filePath,
				outputDirectoryPath,
			)
			if err != nil {
				logger.Logger.Error().Msgf("Error splitting file %s to %s: %v", filePath, outputDirectoryPath, err)

				return
			}

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Generating metadata
			//
			var metadataInfo = structs.MetadataInfoStruct{
				Name:        command.String("name"),
				Description: command.String("description"),
				Path:        absoluteOutputDirectoryPath,
				Lines:       stats.Lines,
				Parts:       stats.Parts,
			}

			if metadataInfo.Name == "" {
				metadataInfo.Name = filepath.Base(filePath)
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Saving metadata
			//
			metadataInfoFile, err := utils.OpenOrCreateDatabase(metadataInfoFilePath)
			if err != nil {
				return
			}
			writer := csv.NewWriter(metadataInfoFile)
			defer writer.Flush()

			record := []string{
				metadataInfo.Name,
				metadataInfo.Description,
				metadataInfo.Path,
				strconv.Itoa(metadataInfo.Lines),
				strconv.Itoa(metadataInfo.Parts),
			}

			if err := writer.Write(record); err != nil {
				logger.Logger.Error().Msgf("Error writing to CSV: %v", err)
				return
			}

			if err := metadataInfoFile.Close(); err != nil {
				logger.Logger.Error().Msgf("Error closing metadataInfo db: %v", err)

				return
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}(inputFile)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	wg.Wait()

	logger.Logger.Info().Msg("Done!")

	return nil
}
