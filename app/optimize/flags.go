package optimize

import (
	ucli "github.com/urfave/cli/v3"
	"runtime"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Flags = []ucli.Flag{
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
	&ucli.BoolFlag{
		Name:    "recursive",
		Aliases: []string{"r"},
		Usage:   "Search recursively",
		Value:   false,
	},
	&ucli.IntFlag{
		Name:     "threads",
		Aliases:  []string{"t"},
		Usage:    "Number of threads to use",
		Value:    int64(runtime.NumCPU()),
		Required: false,
	},
}
