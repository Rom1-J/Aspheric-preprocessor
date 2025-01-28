package main

import (
	"context"
	"github.com/Rom1-J/preprocessor/cli/chunkify"
	"github.com/Rom1-J/preprocessor/cli/extract"
	"github.com/Rom1-J/preprocessor/cli/populate"
	ucli "github.com/urfave/cli/v3"
	"os"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func main() {
	cmd := &ucli.Command{
		Commands: []*ucli.Command{
			chunkify.Command,
			extract.Command,
			populate.Command,
		},
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		return
	}
}
