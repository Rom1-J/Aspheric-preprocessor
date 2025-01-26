package process

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/Rom1-J/preprocessor/logger"
	ucli "github.com/urfave/cli/v3"
)

// IngestAll ingest all file in specified directory
func IngestAll(_ context.Context, command *ucli.Command) {
	logger.SetLoggerLevel(command.Bool("verbose"))
	logger.Logger.Info().Msgf("Log level verbose: %t", command.Bool("verbose"))

	baseURL := command.String("url")
	maxThreads := int(command.Int("threads"))

	paths, err := filepath.Glob(command.String("input") + "/*/_metadata.csv")
	if err != nil {
		logger.Logger.Error().Msgf("Cannot list file in output/*/: %s", err)
		return
	}

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, maxThreads)

	for _, path := range paths {
		logger.Logger.Debug().Msgf("Locking slot for: %s", path)
		semaphore <- struct{}{}
		wg.Add(1)

		go func() {
			defer func() {
				logger.Logger.Debug().Msgf("Releasing slot for: %s", path)
				<-semaphore
				wg.Done()
			}()

			logger.Logger.Info().Msgf("Uploading file %s", path)

			err := ingestCSV(baseURL, path, command.String("collection"))
			if err != nil {
				logger.Logger.Error().Msgf("Cannot ingest file: %s", err)
			}
		}()
	}

	wg.Wait()
}

// ingestCSV upload a metadata csv file on Apache Solr
func ingestCSV(solrURL string, path string, collection string) error {
	url := solrURL + "/" + collection + "/update" + "?commit=true&fieldnames=id,emails,ips,domains&f.emails.split=true&f.emails.separator=|&f.ips.split=true&f.ips.separator=|&f.domains.split=true&f.domains.separator=|&optimize=true"
	logger.Logger.Debug().Msgf("Ingesting file: %s, collection: %s, url: %s", path, collection, url)

	f, err := os.Open(path)
	if err != nil {
		return err
	}

	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			logger.Logger.Error().Msgf("Failed to close file: %v", err)
		}
	}(f)

	req, err := http.NewRequest("POST", url, f)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/csv")

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	return nil
}
