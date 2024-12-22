package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/process"
	"github.com/Rom1-J/preprocessor/structs"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	ucli "github.com/urfave/cli/v3"
	"io"
	"os"
	"path/filepath"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Populate = &ucli.Command{
	Name:  "populate",
	Usage: "Populate metadata to a Neo4j instance.",
	Flags: []ucli.Flag{
		&ucli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "Verbose mode",
			Value:   false,
		},
		&ucli.StringFlag{
			Name:     "input",
			Aliases:  []string{"i"},
			Usage:    "Input directory",
			Required: true,
		},
		&ucli.StringFlag{
			Name:     "username",
			Aliases:  []string{"u"},
			Usage:    "Neo4j username",
			Required: true,
		},
		&ucli.StringFlag{
			Name:     "password",
			Aliases:  []string{"p"},
			Usage:    "Neo4j password",
			Required: true,
		},
		&ucli.StringFlag{
			Name:    "domain",
			Aliases: []string{"d"},
			Usage:   "Neo4j domain",
			Value:   "localhost",
		},
		&ucli.IntFlag{
			Name:    "threads",
			Aliases: []string{"t"},
			Usage:   "Reader threads",
			Value:   16,
		},
	},
	Action: func(ctx context.Context, command *ucli.Command) error {
		logger.SetLoggerLevel(command.Bool("verbose"))
		logger.Logger.Info().Msgf("Log level verbose: %t", command.Bool("verbose"))

		dbDomain := command.String("domain")
		dbUser := command.String("username")
		dbPassword := command.String("password")

		driver, err := neo4j.NewDriverWithContext(
			"neo4j://"+dbDomain,
			neo4j.BasicAuth(dbUser, dbPassword, ""),
		)
		defer func(driver neo4j.DriverWithContext, ctx context.Context) {
			err := driver.Close(ctx)
			if err != nil {
				var msg = fmt.Sprintf("Error closing connection to Neo4j: %v", err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return
			}
		}(driver, ctx)

		err = driver.VerifyConnectivity(ctx)
		if err != nil {
			var msg = fmt.Sprintf("Error connecting to Neo4j: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return nil
		}

		//////////////////

		var metadataFilePath = filepath.Join(command.String("input"), "_metadata.ndjson")
		var metadataInfoFilePath = filepath.Join(command.String("input"), "_info.ndjson")

		metadataFile, err := os.Open(metadataFilePath)
		if err != nil {
			var msg = fmt.Sprintf("Error openning metadata db %s: %v", metadataFilePath, err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return nil
		}
		defer func(metadataFile *os.File) {
			err := metadataFile.Close()
			if err != nil {
				var msg = fmt.Sprintf("Error closing metadata db: %v", err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return
			}
		}(metadataFile)

		metadataInfoFile, err := os.Open(metadataInfoFilePath)
		if err != nil {
			var msg = fmt.Sprintf("Error openning metadata info db %s: %v", metadataInfoFilePath, err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return nil
		}
		defer func(metadataInfoFile *os.File) {
			err := metadataInfoFile.Close()
			if err != nil {
				var msg = fmt.Sprintf("Error closing metadata info db: %v", err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return
			}
		}(metadataInfoFile)
		metadataInfoByte, err := io.ReadAll(metadataInfoFile)
		if err != nil {
			var msg = fmt.Sprintf("Error reading metadata info db %s: %v", metadataInfoFilePath, err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return nil
		}

		var metadataInfo structs.MetadataInfoStruct
		if err := json.Unmarshal(metadataInfoByte, &metadataInfo); err != nil {
			var msg = fmt.Sprintf("Error parsing metadata info db %s: %v", metadataInfoFilePath, err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return nil
		}

		//////////////////

		var wg sync.WaitGroup
		scanner := bufio.NewScanner(metadataFile)
		metadataChan := make(chan structs.MetadataStruct)

		failed := 0

		numWorkers := int(command.Int("threads"))
		for i := 0; i < numWorkers; i++ {
			go func() {
				for metadata := range metadataChan {
					process.Populate(ctx, driver, metadata, metadataInfo, &wg)
				}
			}()
		}

		for scanner.Scan() {
			var metadata structs.MetadataStruct
			if err := json.Unmarshal(scanner.Bytes(), &metadata); err != nil {
				var msg = fmt.Sprintf("Error parsing line: %v\n", err)
				logger.Logger.Error().Msgf(msg)

				failed += 1

				continue
			}

			wg.Add(1)
			metadataChan <- metadata
		}

		close(metadataChan)
		wg.Wait()

		fmt.Println(fmt.Sprintf("Processing completed. (%d failed)", failed))

		return nil
	},
}
