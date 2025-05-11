package populate

import (
	"fmt"
	ucli "github.com/urfave/cli/v3"
	"runtime"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Flags = []ucli.Flag{
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
	&ucli.StringFlag{
		Name:  "prefer",
		Usage: "Preferred metadata",
		Value: "opti",
		Validator: func(s string) error {
			switch strings.ToLower(s) {
			case
				"opti",
				"raw":
				return nil
			}
			return fmt.Errorf("excpected one of opti or raw, got: %s", s)
		},
	},
	&ucli.StringSliceFlag{
		Name:    "solr",
		Aliases: []string{"s"},
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
