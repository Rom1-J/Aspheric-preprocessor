package cli

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/utils"
	"github.com/google/uuid"
	ucli "github.com/urfave/cli/v2"
	"path/filepath"
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
		&ucli.StringFlag{
			Name:     "type",
			Aliases:  []string{"t"},
			Usage:    "Type of file (txt, csv, ndjson, ...)",
			Required: true,
		},
		&ucli.PathFlag{
			Name:     "input",
			Aliases:  []string{"i"},
			Usage:    "Input file directory",
			Required: true,
		},
		&ucli.PathFlag{
			Name:    "output",
			Aliases: []string{"o"},
			Usage:   "Output directory",
			Value:   "./output",
		},
	},
	Args: false,
	Action: func(cCtx *ucli.Context) error {
		logger.SetLoggerLevel(cCtx.Bool("verbose"))

		var outputDirectoryPath = filepath.Join(cCtx.Path("output"), uuid.New().String())

		logger.Logger.Info().Msgf(
			"Processing '%s' as type '%s' in '%s'",
			cCtx.String("input"),
			cCtx.String("type"),
			outputDirectoryPath,
		)

		if err := utils.SplitFile(
			cCtx.Path("input"),
			outputDirectoryPath,
		); err != nil {
			logger.Logger.Error().Msg(err.Error())
			fmt.Println(err.Error())
		}

		return nil
	},
}
