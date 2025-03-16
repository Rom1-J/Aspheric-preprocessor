package extract

import (
	"context"
	"fmt"
	"github.com/Rom1-J/preprocessor/app/extract/logic"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	metadataproto "github.com/Rom1-J/preprocessor/proto/metadata"
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
	inputDirectories := command.StringSlice("directory")
	searchRecursively := command.Bool("recursive")
	overwrite := command.Bool("overwrite")

	for _, directory := range inputDirectories {
		if err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				infoFilePath := filepath.Join(path, "_info.pb")
				if _, err := os.Stat(infoFilePath); err == nil {
					metadataFilePath := filepath.Join(path, "_metadata.pb")

					if _, err := os.Stat(metadataFilePath); err == nil && !overwrite {
						var msg = fmt.Sprintf(
							"Skipping directory '%s', use --overwrite to ignore existing _metadata.pb",
							path,
						)
						logger.Logger.Info().Msgf(msg)

						return nil
					}
					inputList = append(inputList, path)
				}

				if !searchRecursively {
					return filepath.SkipDir
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	logger.Logger.Debug().Msgf("Input directories: %v", inputList)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initialize progress bar
	//
	globalProgress := prog.New("Directories processed", len(inputList))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Extracting from directories
	//
	var wg sync.WaitGroup
	maxThreads := int(command.Int("threads"))
	semaphore := make(chan struct{}, maxThreads)

	for _, inputDirectory := range inputList {
		metadataList, err := logic.ProcessDirectory(globalProgress, &wg, semaphore, inputDirectory, command)
		if err != nil {
			globalProgress.GlobalTracker.IncrementWithError(1)
		} else {
			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Saving metadata
			//
			logger.Logger.Trace().Msgf("Locking slot for saving: %s", inputDirectory)
			semaphore <- struct{}{}
			wg.Add(1)

			go func(ipd string, ml *metadataproto.MetadataList) {
				defer func() {
					logger.Logger.Trace().Msgf("Releasing slot for saving: %s", ipd)
					<-semaphore
					wg.Done()
				}()

				metadataFilePath := filepath.Join(ipd, "_metadata.pb")

				data, err := proto.Marshal(ml)
				if err != nil {
					var msg = fmt.Sprintf("Error encoding metadata %s: %v", metadataFilePath, err)
					logger.Logger.Error().Msg(msg)
					globalProgress.GlobalTracker.IncrementWithError(1)

					return
				}

				err = os.WriteFile(metadataFilePath, data, 0644)
				if err != nil {
					var msg = fmt.Sprintf("Error creating metadata %s: %v", metadataFilePath, err)
					logger.Logger.Error().Msg(msg)
					globalProgress.GlobalTracker.IncrementWithError(1)

					return
				}

				globalProgress.GlobalTracker.Increment(1)
			}(inputDirectory, metadataList)
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	wg.Wait()

	logger.Logger.Info().Msg("Done!")

	return nil
}
