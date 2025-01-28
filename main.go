package main

import (
	"context"
	"fmt"
	"github.com/Rom1-J/preprocessor/cli"
	ucli "github.com/urfave/cli/v3"
	"os"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func main() {
	cmd := &ucli.Command{
		Flags: []ucli.Flag{
			&ucli.BoolFlag{
				Name:    "silent",
				Aliases: []string{"s"},
				Usage:   "Silent mode",
				Value:   false,
			},
			&ucli.StringFlag{
				Name:    "log-level",
				Sources: ucli.EnvVars("LOG_LEVEL"),
				Usage:   "Set log level to print",
				Value:   "none",
				Validator: func(s string) error {
					switch strings.ToLower(s) {
					case
						"none",
						"fatal",
						"error",
						"warn",
						"info",
						"debug",
						"trace":
						return nil
					}
					return fmt.Errorf("excpected one of none, fatal, error, warn, info, debug, trace, got: %s", s)
				},
			},
		},
		Commands: []*ucli.Command{
			cli.Chunkify,
			cli.Extract,
			cli.Populate,
		},
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		return
	}
}
