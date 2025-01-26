package cli

import (
	"context"
	"runtime"

	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/process"
	ucli "github.com/urfave/cli/v3"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Populate = &ucli.Command{
	Name:  "populate",
	Usage: "Populate metadata to a Apache solr instance.",
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
			Usage:    "Input directory",
			Required: true,
		},
		&ucli.StringFlag{
			Name:    "url",
			Aliases: []string{"u"},
			Usage:   "Apache Solr URL",
			Value:   "http://localhost:8983/solr/",
		},
		&ucli.StringFlag{
			Name:    "collection",
			Aliases: []string{"c"},
			Usage:   "Collection name",
			Value:   "aspheric",
		},
		&ucli.IntFlag{
			Name:     "threads",
			Aliases:  []string{"t"},
			Usage:    "Number of threads to use",
			Value:    int64(runtime.NumCPU()),
			Required: false,
		},
	},
	Action: func(ctx context.Context, command *ucli.Command) error {
		logger.SetLoggerLevel(command.Bool("verbose"))
		logger.Logger.Info().Msgf("Log level verbose: %t", command.Bool("verbose"))

		process.IngestAll(ctx, command)

		return nil
	},
}
