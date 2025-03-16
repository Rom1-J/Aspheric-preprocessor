package populate

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
	&ucli.StringSliceFlag{
		Name:    "url",
		Aliases: []string{"u"},
		Usage:   "Apache Solr URLs",
		Value:   []string{"http://localhost:8983/solr/"},
	},
	&ucli.StringFlag{
		Name:    "collection",
		Aliases: []string{"c"},
		Usage:   "Collection name",
		Value:   "BigBoi",
	},
	&ucli.IntFlag{
		Name:     "threads",
		Aliases:  []string{"t"},
		Usage:    "Number of threads to use",
		Value:    int64(runtime.NumCPU()),
		Required: false,
	},
}
