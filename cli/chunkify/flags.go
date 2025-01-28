package chunkify

import (
	"github.com/Rom1-J/preprocessor/constants"
	ucli "github.com/urfave/cli/v3"
	"runtime"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Flags = []ucli.Flag{
	&ucli.BoolFlag{
		Name:    "verbose",
		Aliases: []string{"v"},
		Usage:   "Verbose mode",
		Value:   false,
	},
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
}
