package cli

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/process"
	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v8"
	ucli "github.com/urfave/cli/v3"
	"io"
	"math"
	"net/http"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Populate = &ucli.Command{
	Name:  "populate",
	Usage: "Populate metadata to a Elasticsearch instance.",
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
			Usage:    "Elasticsearch username",
			Required: false,
		},
		&ucli.StringFlag{
			Name:     "password",
			Aliases:  []string{"p"},
			Usage:    "Elasticsearch password",
			Required: false,
		},
		&ucli.StringFlag{
			Name:     "api-key",
			Usage:    "Elasticsearch api key",
			Required: false,
		},
		&ucli.StringFlag{
			Name:    "domain",
			Aliases: []string{"d"},
			Usage:   "Elasticsearch domain",
			Value:   "https://localhost:9200",
		},
		&ucli.BoolFlag{
			Name:  "ignore-ssl",
			Usage: "Ignore SSL certificate validation",
			Value: false,
		},
		// todo: arg to specify CA path
		&ucli.IntFlag{
			Name:     "threads",
			Aliases:  []string{"t"},
			Usage:    "Number of threads to use",
			Value:    int64(runtime.NumCPU()),
			Required: false,
		},
		&ucli.IntFlag{
			Name:     "max-retries",
			Usage:    "Amount of retries to make before failing",
			Value:    5,
			Required: false,
		},
	},
	Action: func(ctx context.Context, command *ucli.Command) error {
		logger.SetLoggerLevel(command.Bool("verbose"))
		logger.Logger.Info().Msgf("Log level verbose: %t", command.Bool("verbose"))

		inputDirectory := command.String("input")

		dbDomain := command.String("domain")

		dbUser := command.String("username")
		dbPassword := command.String("password")
		dbApiKey := command.String("api-key")

		config := elasticsearch.Config{
			Addresses: []string{dbDomain},
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: command.Bool("ignore-ssl"),
				},
			},
			RetryOnStatus: []int{502, 503, 504, 429},
			MaxRetries:    int(command.Int("threads")),
			RetryBackoff: func(i int) time.Duration {
				d := time.Duration(math.Exp2(float64(i))) * time.Second
				fmt.Printf("Attempt: %d | Sleeping for %s...\n", i, d)

				return d
			},
		}

		if dbApiKey != "" {
			if dbUser != "" || dbPassword != "" {
				var msg = fmt.Sprintf("Username and password must not be set if api-key is provided")
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return fmt.Errorf(msg)
			}
			config.APIKey = dbApiKey
		} else {
			if dbUser == "" || dbPassword == "" {
				var msg = fmt.Sprintf("Username and password must be set if api-key is not provided")
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return fmt.Errorf(msg)
			}
			config.Username = dbUser
			config.Password = dbPassword
		}

		client, err := elasticsearch.NewClient(config)
		if err != nil {
			var msg = fmt.Sprintf("Error establishing connection to Elasticsearch: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return fmt.Errorf(msg)
		}

		res, err := client.Cluster.Health(
			client.Cluster.Health.WithContext(context.Background()),
		)
		if err != nil {
			var msg = fmt.Sprintf("Error getting cluster health of Elasticsearch: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return fmt.Errorf(msg)
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				var msg = fmt.Sprintf("Error closing cluster health of Elasticsearch: %v", err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)
			}
		}(res.Body)

		if res.IsError() {
			var msg = fmt.Sprintf("Error response: %s", res.String())
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return fmt.Errorf(msg)
		}

		paths, err := filepath.Glob(filepath.Join(inputDirectory, "**", "_metadata", "*"))
		if err != nil {
			var msg = fmt.Sprintf("Error getting _metadata directories: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return fmt.Errorf(msg)
		}

		bucketFiles := make(map[string][]string)
		for _, path := range paths {
			bucketUUID := filepath.Base(filepath.Dir(filepath.Dir(path)))
			bucketFiles[bucketUUID] = append(bucketFiles[bucketUUID], path)
		}

		logger.Logger.Info().Msgf("Found %d metadata files", len(paths))

		var wg sync.WaitGroup
		maxThreads := int(command.Int("threads"))
		semaphore := make(chan struct{}, maxThreads)

		var flushed uint64 = 0
		var failed uint64 = 0
		var failedBuckets []string

		for bucketUUID, filesPath := range bucketFiles {
			logger.Logger.Debug().Msgf("Locking slot for: %s", bucketUUID)
			semaphore <- struct{}{}

			wg.Add(1)
			go func(bucketUUID string, filePaths []string) {
				defer func() {
					logger.Logger.Debug().Msgf("Releasing slot for: %s", bucketUUID)
					<-semaphore
					wg.Done()
				}()

				biStats, err := process.Populate(client, bucketUUID, filePaths)
				if err != nil {
					var msg = fmt.Sprintf("Error processing file in %s: %v", bucketUUID, err)
					logger.Logger.Error().Msgf(msg)
					fmt.Println(msg)

					failedBuckets = append(failedBuckets, bucketUUID)
				}

				flushed += biStats.NumFlushed
				failed += biStats.NumFailed
			}(bucketUUID, filesPath)
		}

		wg.Wait()

		var msg = fmt.Sprintf(
			"%s buckets added (%s failed), %s new documents with %s errors",
			humanize.Comma(int64(len(bucketFiles)-len(failedBuckets))),
			humanize.Comma(int64(len(failedBuckets))),
			humanize.Comma(int64(flushed)),
			humanize.Comma(int64(failed)),
		)

		logger.Logger.Info().Msg(msg)
		fmt.Println(msg)

		return nil
	},
}
