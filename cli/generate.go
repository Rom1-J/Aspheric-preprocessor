package cli

import (
	"context"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/process"
	"github.com/Rom1-J/preprocessor/structs"
	"github.com/Rom1-J/preprocessor/utils"
	"github.com/google/uuid"
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
	Usage: "Generate metadata for a file by extracting fragments.",
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
			Usage:    "Input file",
			Required: true,
		},
		&ucli.StringFlag{
			Name:     "output",
			Aliases:  []string{"o"},
			Usage:    "Output directory",
			Value:    "./output",
			Required: false,
		},
		&ucli.StringFlag{
			Name:        "name",
			Usage:       "Name to be registered",
			DefaultText: "",
			Required:    false,
		},
		&ucli.StringFlag{
			Name:        "description",
			Usage:       "Description to be registered",
			DefaultText: "",
			Required:    false,
		},
	},
	Action: func(ctx context.Context, command *ucli.Command) error {
		logger.SetLoggerLevel(command.Bool("verbose"))
		logger.Logger.Info().Msgf("Log level verbose: %t", command.Bool("verbose"))

		var outputDirectoryPath = filepath.Join(command.String("output"), uuid.New().String())
		var metadataFilePath = filepath.Join(outputDirectoryPath, "_metadata.csv")
		var metadataInfoFilePath = filepath.Join(outputDirectoryPath, "_info.csv")

		logger.Logger.Info().Msgf(
			"Processing '%s' in '%s'",
			command.String("input"),
			outputDirectoryPath,
		)

		if err := utils.SplitFile(
			command.String("input"),
			outputDirectoryPath,
		); err != nil {
			logger.Logger.Error().Msg(err.Error())
			fmt.Println(err.Error())
		} else {
			absoluteOutputDirectoryPath, err := filepath.Abs(outputDirectoryPath)
			if err != nil {
				var msg = fmt.Sprintf("Error getting absolute output path: %v", err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return nil
			}

			var metadataInfo = structs.MetadataInfoStruct{
				Name:        command.String("name"),
				Description: command.String("description"),
				Path:        absoluteOutputDirectoryPath,
			}

			if metadataInfo.Name == "" {
				metadataInfo.Name = filepath.Base(command.String("input"))
			}

			metadataInfoFile, err := utils.OpenOrCreateDatabase(metadataInfoFilePath)
			if err != nil {
				return nil
			}
			if err = process.SaveMetadataInfo(metadataInfoFile, metadataInfo); err != nil {
				return err
			}
			if err := metadataInfoFile.Close(); err != nil {
				var msg = fmt.Sprintf("Error closing metadataInfo db: %v", err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return nil
			}

			var metadataChan = make(chan structs.MetadataStruct)

			var wg sync.WaitGroup
			maxThreads := int(command.Int("threads"))
			semaphore := make(chan struct{}, maxThreads)

			var paths []string
			err = filepath.Walk(outputDirectoryPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					var msg = fmt.Sprintf("Error accessing path %s: %v", path, err)
					logger.Logger.Error().Msgf(msg)
					fmt.Println(msg)
					return nil
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

				return nil
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

					return nil
				}
			}

			if err := metadataFile.Close(); err != nil {
				var msg = fmt.Sprintf("Error closing metadata db: %v", err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return nil
			}

			logger.Logger.Info().Msg("Processed all files")
			fmt.Println("All files processed.")
		}

		return nil
	},
}
