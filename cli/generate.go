package cli

import (
	"context"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/process"
	"github.com/Rom1-J/preprocessor/structs"
	"github.com/Rom1-J/preprocessor/utils"
	ucli "github.com/urfave/cli/v3"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Extract = &ucli.Command{
	Name:  "extract",
	Usage: "Extract metadata from .part in given directory.",
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
		&ucli.StringFlag{
			Name:     "input",
			Aliases:  []string{"i"},
			Usage:    "Input directory",
			Required: true,
		},
	},
	Action: func(ctx context.Context, command *ucli.Command) error {
		logger.SetLoggerLevel(command.Bool("verbose"))
		logger.Logger.Info().Msgf("Log level verbose: %t", command.Bool("verbose"))

		inputDirectory := command.String("input")

		metadataFilePath := filepath.Join(inputDirectory, "_metadata.csv")

		logger.Logger.Info().Msgf(
			"Processing '%s'",
			inputDirectory,
		)

		absoluteOutputDirectoryPath, err := filepath.Abs(inputDirectory)
		if err != nil {
			var msg = fmt.Sprintf("Error getting absolute output path: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return fmt.Errorf(msg)
		}

		var metadataInfo = structs.MetadataInfoStruct{
			Name:        command.String("name"),
			Description: command.String("description"),
			Path:        absoluteOutputDirectoryPath,
		}

		if metadataInfo.Name == "" {
			metadataInfo.Name = filepath.Base(inputDirectory)
		}

		var metadataChan = make(chan structs.MetadataStruct)

		var wg sync.WaitGroup
		maxThreads := int(command.Int("threads"))
		semaphore := make(chan struct{}, maxThreads)

		var paths []string
		err = filepath.Walk(inputDirectory, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				var msg = fmt.Sprintf("Error accessing path %s: %v", path, err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return fmt.Errorf(msg)
			}

			if !info.IsDir() && filepath.Ext(path) != ".csv" {
				paths = append(paths, path)
			}

			return nil
		})

		if err != nil {
			var msg = fmt.Sprintf("Error walking the directory: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return fmt.Errorf(msg)
		}

		for _, path := range paths {
			logger.Logger.Debug().Msgf("Locking slot for: %s", path)
			semaphore <- struct{}{}
			wg.Add(1)
			go func(filePath string) {
				metadataList, err := process.Extractor(filePath)
				if err != nil {
					var msg = fmt.Sprintf("Error starting extractor for file %s: %v", filePath, err)
					logger.Logger.Error().Msgf(msg)
					fmt.Println(msg)
					return
				}

				logger.Logger.Debug().Msgf("Releasing slot for: %s", path)
				<-semaphore

				for _, metadata := range metadataList {
					metadataChan <- metadata
				}

				wg.Done()
			}(path)
		}

		go func() {
			wg.Wait()
			close(metadataChan)
		}()

		metadataFile, err := utils.OpenOrCreateDatabase(metadataFilePath)
		if err != nil {
			return nil
		}

		for metadata := range metadataChan {
			if err := process.SaveMetadata(metadataFile, metadata); err != nil {
				var msg = fmt.Sprintf("Error saving metadata: %v", err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return fmt.Errorf(msg)
			}
		}

		if err := metadataFile.Close(); err != nil {
			var msg = fmt.Sprintf("Error closing metadata db: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return fmt.Errorf(msg)
		}

		logger.Logger.Info().Msg("Processed all files")
		fmt.Println("All files processed.")

		return nil
	},
}
