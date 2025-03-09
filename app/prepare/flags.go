package prepare

import (
	ucli "github.com/urfave/cli/v3"
	"runtime"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var now = time.Now().Format("2006-01-02 15:04:05")

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
		Name:        "date",
		Usage:       "Origin date",
		DefaultText: now,
		Value:       now,
		Required:    false,
	},
}
