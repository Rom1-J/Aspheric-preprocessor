package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Rom1-J/preprocessor/cli"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	ucli "github.com/urfave/cli/v3"
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
			&ucli.BoolFlag{
				Name:  "progress",
				Usage: "Show progressbar",
				Value: false,
			},
			&ucli.StringFlag{
				Name:    "log-level",
				Sources: ucli.EnvVars("LOG_LEVEL"),
				Usage:   "Set log level to print",
				Aliases: []string{"l"},
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

	if prog.GlobalProgress.Pw != nil && prog.GlobalProgress.Pw.IsRenderInProgress() {
		prog.GlobalProgress.GlobalTracker.MarkAsDone()
		time.Sleep(time.Millisecond * 100)
		prog.GlobalProgress.Pw.Stop()
	}
}
