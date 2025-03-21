package extract

import (
	ucli "github.com/urfave/cli/v3"
	"runtime"
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
		Name:  "overwrite",
		Usage: "Overwrite existing _metadata.pb",
		Value: false,
	},
}
