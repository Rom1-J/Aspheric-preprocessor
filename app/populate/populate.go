package populate

import (
	"context"
	"github.com/Rom1-J/preprocessor/logger"
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
		inputList     []string
		currentThread int

		err error
	)

	//solrUrls := command.StringSlice("url")
	//solrCollection := command.String("collection")

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Retrieving input descriptors
	// todo: dedupe code logic
	//
	inputFiles := command.StringSlice("input")
	inputDirectories := command.StringSlice("directory")
	searchRecursively := command.Bool("recursive")

	inputList = append(inputList, inputFiles...)

	for _, directory := range inputDirectories {
		if err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				metadataFilePath := filepath.Join(path, "_metadata.pb")
				if _, err := os.Stat(metadataFilePath); err == nil {
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

	logger.Logger.Debug().Msgf("Input files: %v", inputList)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Processing paths
	//
	var wg sync.WaitGroup
	maxThreads := int(command.Int("threads"))
	semaphore := make(chan struct{}, maxThreads)

	for _, path := range inputList {
		logger.Logger.Trace().Msgf("Locking slot for: %s", path)

		semaphore <- struct{}{}
		wg.Add(1)

		go func() {
			currentThread++

			defer func() {
				logger.Logger.Trace().Msgf("Releasing slot for: %s", path)

				<-semaphore
				wg.Done()
			}()

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Ingesting _metadata.pb
			//
			//if err := logic.IngestCSV(path, solrUrls[currentThread%len(solrUrls)], solrCollection); err != nil {
			//	logger.Logger.Error().Msgf("Cannot ingest file '%s': %s", path, err)
			//}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}()
	}

	wg.Wait()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return nil
}
