package cli

import (
	"context"
	"github.com/Rom1-J/preprocessor/process"
	"github.com/Rom1-J/preprocessor/structs"
	"github.com/Rom1-J/preprocessor/utils"
	"github.com/google/uuid"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/Rom1-J/preprocessor/constants"
	"github.com/Rom1-J/preprocessor/logger"
	ucli "github.com/urfave/cli/v3"
	"os"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Chunkify = &ucli.Command{
	Name:  "chunkify",
	Usage: "Chunkify a file in small parts.",
	Flags: []ucli.Flag{
		&ucli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "Verbose mode",
			Value:   false,
		},
		&ucli.StringSliceFlag{
			Name:     "input",
			Aliases:  []string{"i"},
			Usage:    "Input file",
			Required: false,
		},
		&ucli.StringSliceFlag{
			Name:     "directory",
			Aliases:  []string{"d"},
			Usage:    "Input directory",
			Required: false,
		},
		&ucli.StringFlag{
			Name:     "output",
			Aliases:  []string{"o"},
			Usage:    "Output directory",
			Value:    "./output",
			Required: false,
		},
		&ucli.IntFlag{
			Name:     "threads",
			Aliases:  []string{"t"},
			Usage:    "Number of threads to use",
			Value:    int64(runtime.NumCPU()),
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
		&ucli.IntFlag{
			Name:     "size",
			Aliases:  []string{"s"},
			Usage:    "Size of each chunk in bytes",
			Value:    constants.ChunkSize,
			Required: false,
		},
	},
	Action: func(ctx context.Context, command *ucli.Command) error {
		logger.SetLoggerLevel(command.Bool("verbose"))
		logger.Logger.Info().Msgf("Log level verbose: %t", command.Bool("verbose"))

		var (
			inputList []string

			err error
		)

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Retrieving input descriptors
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
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Chunkify files
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
				}()

				// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
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
				// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

				stats, err := utils.SplitFile(
					filePath,
					outputDirectoryPath,
				)
				if err != nil {
					logger.Logger.Error().Msgf("Error splitting file %s to %s: %v", filePath, outputDirectoryPath, err)

					return
				}

				// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
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
				// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

				// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
				//
				// Saving metadata
				//
				metadataInfoFile, err := utils.OpenOrCreateDatabase(metadataInfoFilePath)
				if err != nil {
					return
				}
				if err = process.SaveMetadataInfo(metadataInfoFile, metadataInfo); err != nil {
					return
				}
				if err := metadataInfoFile.Close(); err != nil {
					logger.Logger.Error().Msgf("Error closing metadataInfo db: %v", err)

					return
				}
				// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
			}(inputFile)
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		go func() {
			wg.Wait()
		}()

		logger.Logger.Info().Msg("Done!")

		return nil
	},
}
