package populate

import (
	"context"
	"github.com/Rom1-J/preprocessor/app/populate/logic"
	"github.com/Rom1-J/preprocessor/app/populate/structs"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	ucli "github.com/urfave/cli/v3"
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
	searchRecursively := command.Bool("recursive")

	var metadataFileName string
	if command.String("prefer") == "opti" {
		metadataFileName = "_metadata.opti.pb"
	} else {
		metadataFileName = "_metadata.pb"
	}

	inputList = append(inputList, inputFiles...)

	for _, directory := range inputDirectories {
		if err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				metadataFilePath := filepath.Join(path, metadataFileName)
				if _, err := os.Stat(metadataFilePath); err == nil {
					inputList = append(inputList, metadataFilePath)
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

	logger.Logger.Debug().Msgf("Input files: %v", inputList)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initialize progress bar
	//
	globalProgress := prog.New("Directories processed", len(inputList))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Processing paths
	//
	var wg sync.WaitGroup
	maxThreads := int(command.Int("threads"))
	semaphore := make(chan struct{}, maxThreads)

	for i, inputMetadataPb := range inputList {
		logger.Logger.Trace().Msgf("Locking slot for saving: %s", inputMetadataPb)
		semaphore <- struct{}{}
		wg.Add(1)

		go func(ipmpb string) {
			defer func() {
				logger.Logger.Trace().Msgf("Releasing slot for saving: %s", ipmpb)

				globalProgress.GlobalTracker.Increment(1)
				<-semaphore
				wg.Done()
			}()

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Ingesting _metadata.pb
			//
			if err := logic.ProcessMetadataPb(
				globalProgress,
				ipmpb,
				structs.SolrOptsStruct{
					Address:    command.StringSlice("solr")[i%len(command.StringSlice("solr"))],
					Collection: command.String("collection"),
				},
			); err != nil {
				logger.Logger.Error().Msgf("Cannot ingest file '%s': %s", ipmpb, err)
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}(inputMetadataPb)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	wg.Wait()

	logger.Logger.Info().Msg("Done!")

	return nil
}
