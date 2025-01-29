package logic

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	"github.com/Rom1-J/preprocessor/utils"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/urfave/cli/v3"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func ProcessDirectory(
	globalProgress prog.ProgressOptsStruct,
	wg *sync.WaitGroup,
	semaphore chan struct{},
	inputDirectory string,
	command *cli.Command,
) error {
	metadataFilePath := filepath.Join(inputDirectory, "_metadata.csv")
	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Skip if _metadata.csv exists
	//
	if _, err := os.Stat(metadataFilePath); err == nil {
		if !command.Bool("overwrite") {
			message := fmt.Sprintf(
				"Skipping directory '%s', use --overwrite to ignore existing _metadata.csv",
				inputDirectory,
			)
			globalProgress.Pw.Log(message)
			logger.Logger.Info().Msgf(message)

			return nil
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Opening output descriptor
	//
	metadataFileWriter, err := utils.ParallelCsvWriter(metadataFilePath)
	if err != nil {
		return err
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Retrieving .partX paths
	//
	var paths []string
	err = filepath.Walk(inputDirectory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Logger.Error().Msgf("Error accessing path %s: %v", path, err)
			return nil
		}

		if !info.IsDir() && filepath.Ext(path) != ".csv" {
			paths = append(paths, path)
		}

		return nil
	})

	if err != nil {
		logger.Logger.Error().Msgf("Error walking the directory: %v", err)
		return nil
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initialize tracker
	//
	tracker := progress.Tracker{
		Message: "Processing directory " + filepath.Base(inputDirectory),
		Total:   int64(len(paths)),
	}
	globalProgress.Pw.AppendTracker(&tracker)
	globalProgress.GlobalTracker.UpdateTotal(globalProgress.GlobalTracker.Total + int64(len(paths)))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Processing paths
	//
	for _, path := range paths {
		logger.Logger.Debug().Msgf("Locking slot for: %s", path)
		semaphore <- struct{}{}
		wg.Add(1)

		go func(filePath string) {
			defer func() {
				tracker.Increment(1)
				globalProgress.GlobalTracker.Increment(1)

				logger.Logger.Debug().Msgf("Releasing slot for: %s", filePath)
				<-semaphore
				wg.Done()
			}()

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Extracting metadata from .partX
			//
			metadataChan, err := Parse(path)
			if err != nil {
				logger.Logger.Error().Msgf("Error starting extractor for file %s: %v", path, err)
				return
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Saving metadata
			//
			for metadata := range metadataChan {
				metadataFileWriter.Write([]string{
					metadata.File,
					strings.Join(metadata.Emails, "|"),
					strings.Join(metadata.IPs, "|"),
					strings.Join(metadata.Domains, "|"),
				})
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}(path)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Closing output descriptor
	//
	go func() {
		wg.Wait()

		err = metadataFileWriter.Close()
		if err != nil {
			logger.Logger.Error().Msgf("Error closing metadata db: %v", err)

			return
		}

		tracker.MarkAsDone()
	}()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return nil
}
