package cli

import (
	"context"
	"github.com/Rom1-J/preprocessor/process"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/Rom1-J/preprocessor/logger"
	ucli "github.com/urfave/cli/v3"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Populate = &ucli.Command{
	Name:  "populate",
	Usage: "Populate metadata to a Apache solr instance (must be run AFTER extract).",
	Flags: []ucli.Flag{
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
	},
	Action: populate,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func populate(ctx context.Context, command *ucli.Command) error {
	logger.SetLoggerLevel(command.Bool("verbose"))
	logger.Logger.Info().Msgf("Log level verbose: %t", command.Bool("verbose"))

	var (
		inputList     []string
		currentThread int

		err error
	)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Retrieving input descriptors
	// todo: dedupe code logic
	//
	solrUrls := command.StringSlice("url")
	solrCollection := command.String("collection")

	inputFiles := command.StringSlice("input")
	inputDirectories := command.StringSlice("directory")

	inputList = append(inputList, inputFiles...)

	for _, directory := range inputDirectories {
		if err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				metadataFilePath := filepath.Join(path, "_metadata.csv")
				if _, err := os.Stat(metadataFilePath); err == nil {
					inputList = append(inputList, metadataFilePath)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	logger.Logger.Debug().Msgf("Input files: %v", inputList)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Processing paths
	//
	var wg sync.WaitGroup
	maxThreads := int(command.Int("threads"))
	semaphore := make(chan struct{}, maxThreads)

	for _, path := range inputList {
		logger.Logger.Debug().Msgf("Locking slot for: %s", path)
		semaphore <- struct{}{}
		wg.Add(1)

		go func() {
			currentThread++
			
			defer func() {
				logger.Logger.Debug().Msgf("Releasing slot for: %s", path)
				<-semaphore
				wg.Done()
			}()

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Ingesting _metadata.csv
			//
			if err := process.IngestCSV(path, solrUrls[currentThread%len(solrUrls)], solrCollection); err != nil {
				logger.Logger.Error().Msgf("Cannot ingest file '%s': %s", path, err)
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}()
	}

	wg.Wait()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return nil
}
