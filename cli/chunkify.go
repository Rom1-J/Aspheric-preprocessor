package cli

import (
	"context"
	"fmt"
	"github.com/Rom1-J/preprocessor/constants"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/process"
	"github.com/Rom1-J/preprocessor/structs"
	"github.com/Rom1-J/preprocessor/utils"
	"github.com/google/uuid"
	ucli "github.com/urfave/cli/v3"
	"path/filepath"
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

		inputFile := command.String("input")
		outputDirectoryPath := filepath.Join(command.String("output"), uuid.New().String())

		metadataInfoFilePath := filepath.Join(outputDirectoryPath, "_info.csv")

		logger.Logger.Info().Msgf(
			"Chunkifing '%s' in '%s'",
			inputFile,
			outputDirectoryPath,
		)

		stats, err := utils.SplitFile(
			inputFile,
			outputDirectoryPath,
		)
		if err != nil {
			var msg = fmt.Sprintf("Error splitting file %s to %s: %v", inputFile, outputDirectoryPath, err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return fmt.Errorf(msg)
		}

		absoluteOutputDirectoryPath, err := filepath.Abs(outputDirectoryPath)
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
			Lines:       stats.Lines,
			Parts:       stats.Parts,
		}

		if metadataInfo.Name == "" {
			metadataInfo.Name = filepath.Base(inputFile)
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

			return fmt.Errorf(msg)
		}

		logger.Logger.Info().Msg("Processed all files")
		fmt.Println("All files processed.")

		return nil
	},
}
