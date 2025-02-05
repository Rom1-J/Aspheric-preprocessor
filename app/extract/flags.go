package extract

import (
	"runtime"

	ucli "github.com/urfave/cli/v3"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Flags = []ucli.Flag{
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
	&ucli.BoolFlag{
		Name:    "overwrite",
		Aliases: []string{"f"},
		Usage:   "Overwrite existing _metadata.csv",
		Value:   false,
	},
	&ucli.StringSliceFlag{
		Name:    "module",
		Aliases: []string{"m"},
		Usage:   "List of activated modules (comma separate) (all by default)",
	},
}
