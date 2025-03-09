package prepare

import (
	"context"
	"fmt"
	"github.com/Rom1-J/preprocessor/app/prepare/logic"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	"github.com/google/uuid"
	ucli "github.com/urfave/cli/v3"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
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

	logger.Logger.Debug().Msgf("Input files: %v", inputList)

	logger.Logger.Info().Msgf("Chunkifing %d files", len(inputList))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Create output directory
	//
	if err = os.MkdirAll(command.String("output"), 0755); err != nil {
		var msg = fmt.Sprintf("Failed to create output directory: %v", err)
		logger.Logger.Error().Msg(msg)

		return nil
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initialize progress bar
	//
	globalProgress := prog.New("Files processed", len(inputList))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Preparing files
	//
	var wg sync.WaitGroup
	maxThreads := int(command.Int("threads"))
	semaphore := make(chan struct{}, maxThreads)

	for _, inputFile := range inputList {
		logger.Logger.Debug().Msgf("Locking slot for: %s", inputFile)
		semaphore <- struct{}{}
		wg.Add(1)

		go func(filePath string) {
			defer func() {
				logger.Logger.Debug().Msgf("Releasing slot for: %s", filePath)
				<-semaphore
				wg.Done()

				globalProgress.GlobalTracker.Increment(1)
			}()

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Retrieving output descriptor
			//
			outputDirectoryPath := filepath.Join(command.String("output"), uuid.New().String())
			metadataInfoFilePath := filepath.Join(outputDirectoryPath, "_info.pb")
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

			if err = os.MkdirAll(outputDirectoryPath, 0755); err != nil {
				var msg = fmt.Sprintf("Failed to create output directory: %v", err)
				logger.Logger.Error().Msg(msg)

				return
			}

			metadataInfo, err := logic.PrepareFile(
				globalProgress,
				command.String("date"),
				filePath,
				outputDirectoryPath,
			)
			if err != nil {
				logger.Logger.Error().Msgf("Error preparing file %s to %s: %v", filePath, outputDirectoryPath, err)

				return
			}

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Saving metadata
			//
			data, err := proto.Marshal(metadataInfo)
			if err != nil {
				logger.Logger.Error().Msgf("Error encoding metadata %s: %v", metadataInfoFilePath, err)
			}

			err = os.WriteFile(metadataInfoFilePath, data, 0644)
			if err != nil {
				logger.Logger.Error().Msgf("Error creating metadata %s: %v", metadataInfoFilePath, err)
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}(inputFile)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	wg.Wait()

	logger.Logger.Info().Msg("Done!")

	return nil
}
