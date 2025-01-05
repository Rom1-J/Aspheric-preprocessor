package cli

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/elastic/go-elasticsearch/v8"
	ucli "github.com/urfave/cli/v3"
	"io"
	"net/http"
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
		dbApiKey := command.String("api-key")

		var config elasticsearch.Config

		if dbApiKey != "" {
			if dbUser != "" || dbPassword != "" {
				var msg = fmt.Sprintf("Username and password must not be set if api-key is provided")
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return fmt.Errorf(msg)
			}
			config = elasticsearch.Config{
				Addresses: []string{dbDomain},
				APIKey:    dbApiKey,
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: command.Bool("ignore-ssl"),
					},
				},
			}
		} else {
			if dbUser == "" || dbPassword == "" {
				var msg = fmt.Sprintf("Username and password must be set if api-key is not provided")
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return fmt.Errorf(msg)
			}
			config = elasticsearch.Config{
				Addresses: []string{dbDomain},
				Username:  dbUser,
				Password:  dbPassword,
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: command.Bool("ignore-ssl"),
					},
				},
			}
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

		return nil
	},
}
