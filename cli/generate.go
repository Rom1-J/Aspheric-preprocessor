package cli

import (
	"context"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/process"
	"github.com/Rom1-J/preprocessor/utils"
	ucli "github.com/urfave/cli/v3"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Generate = &ucli.Command{
	Name:  "generate",
	Usage: "Generate metadata from .partX in given directory.",
	Flags: []ucli.Flag{
		&ucli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "Verbose mode",
			Value:   false,
		},
		&ucli.IntFlag{
			Name:     "threads",
			Aliases:  []string{"t"},
			Usage:    "Number of threads to use",
			Value:    int64(runtime.NumCPU()),
			Required: false,
		},
		&ucli.StringSliceFlag{
			Name:     "directory",
			Aliases:  []string{"d"},
			Usage:    "Input directory",
			Required: true,
		},
		&ucli.BoolFlag{
			Name:    "recursive",
			Aliases: []string{"r"},
			Usage:   "Search recursively",
			Value:   false,
		},
	},
	Action: generate,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func generate(ctx context.Context, command *ucli.Command) error {
	logger.SetLoggerLevel(command.Bool("verbose"))
	logger.Logger.Info().Msgf("Log level verbose: %t", command.Bool("verbose"))

	var (
		inputList []string

		err error
	)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Retrieving input descriptors
	//
	inputDirectories := command.StringSlice("directory")
	searchRecursively := command.Bool("recursive")

	for _, directory := range inputDirectories {
		if err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				infoFilePath := filepath.Join(path, "_info.csv")
				if _, err := os.Stat(infoFilePath); err == nil {
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
	// Extracting from directories
	//
	var wg sync.WaitGroup
	maxThreads := int(command.Int("threads"))
	semaphore := make(chan struct{}, maxThreads)

	for _, inputDirectory := range inputList {
		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Opening output descriptor
		//
		metadataFilePath := filepath.Join(inputDirectory, "_metadata.csv")

		metadataFile, err := utils.OpenOrCreateDatabase(metadataFilePath)
		if err != nil {
			continue
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
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
			continue
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Processing paths
		//
		for _, path := range paths {
			logger.Logger.Debug().Msgf("Locking slot for: %s", path)
			semaphore <- struct{}{}
			wg.Add(1)

			go func(filePath string) {
				defer func() {
					logger.Logger.Debug().Msgf("Releasing slot for: %s", filePath)
					<-semaphore
					wg.Done()
				}()

				// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
				//
				// Extracting metadata from .partX
				//
				metadataChan, err := process.Extractor(path)
				if err != nil {
					logger.Logger.Error().Msgf("Error starting extractor for file %s: %v", path, err)
					return
				}
				// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

				// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
				//
				// Saving metadata
				//
				for metadata := range metadataChan {
					if err := process.SaveMetadata(metadataFile, metadata); err != nil {
						logger.Logger.Error().Msgf("Error saving metadata: %v", err)

						return
					}
				}
				// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
			}(path)
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Closing  output descriptor
		//
		go func() {
			wg.Wait()

			err = metadataFile.Close()
			if err != nil {
				logger.Logger.Error().Msgf("Error closing metadata db: %v", err)

				return
			}
		}()
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	logger.Logger.Info().Msg("Done!")

	return nil
}
