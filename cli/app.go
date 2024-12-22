package cli

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/process"
	"github.com/Rom1-J/preprocessor/structs"
	"github.com/Rom1-J/preprocessor/utils"
	"github.com/google/uuid"
	ucli "github.com/urfave/cli/v2"
	"os"
	"path/filepath"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var App = &ucli.App{
	Flags: []ucli.Flag{
		&ucli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "Verbose mode",
			Value:   false,
		},
		&ucli.PathFlag{
			Name:     "input",
			Aliases:  []string{"i"},
			Usage:    "Input file directory",
			Required: true,
		},
		&ucli.PathFlag{
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
	Args: false,
	Action: func(cCtx *ucli.Context) error {
		logger.SetLoggerLevel(cCtx.Bool("verbose"))

		var outputDirectoryPath = filepath.Join(cCtx.Path("output"), uuid.New().String())
		var metadataFilePath = filepath.Join(outputDirectoryPath, "_metadata.db")

		logger.Logger.Info().Msgf(
			"Processing '%s' in '%s'",
			cCtx.String("input"),
			outputDirectoryPath,
		)

		if err := utils.SplitFile(
			cCtx.Path("input"),
			outputDirectoryPath,
		); err != nil {
			logger.Logger.Error().Msg(err.Error())
			fmt.Println(err.Error())
		} else {
			var name = cCtx.String("name")
			if name == "" {
				name = filepath.Base(cCtx.String("input"))
			}
			var description = cCtx.String("description")

			if err := process.InitDatabase(metadataFilePath); err != nil {
				var msg = fmt.Sprintf("Error initializing metatadata database path %s: %v", metadataFilePath, err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return nil
			}

			var metadataChan = make(chan structs.MetadataStruct)

			var wg sync.WaitGroup
			err = filepath.Walk(outputDirectoryPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					var msg = fmt.Sprintf("Error accessing path %s: %v", path, err)
					logger.Logger.Error().Msgf(msg)
					fmt.Println(msg)

					return nil
				}

				if info.IsDir() || filepath.Base(path) == "_metadata.db" {
					return nil
				}

				wg.Add(1)
				go func(filePath string) {
					defer wg.Done()
					metadataList, err := process.Extractor(filePath, name, description)

					if err != nil {
						var msg = fmt.Sprintf("Error starting extractor for file %s: %v", filePath, err)
						logger.Logger.Error().Msgf(msg)
						fmt.Println(msg)

						return
					}

					for _, metadata := range metadataList {
						metadataChan <- metadata
					}
				}(path)

				return nil
			})

			if err != nil {
				var msg = fmt.Sprintf("Error walking the directory: %v", err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return nil
			}

			db, err := process.OpenDatabase(metadataFilePath)
			if err != nil {
				return nil
			}

			for metadata := range metadataChan {
				if err := process.SaveMetadata(db, metadata); err != nil {
					var msg = fmt.Sprintf("Error saving metadata: %v", err)
					logger.Logger.Error().Msgf(msg)
					fmt.Println(msg)

					return nil
				}
			}

			wg.Wait()

			logger.Logger.Info().Msg("Processed all files")
			fmt.Println("All files processed.")
		}

		return nil
	},
}
