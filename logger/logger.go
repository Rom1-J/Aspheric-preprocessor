package logger

import (
	"github.com/rs/zerolog"
	ucli "github.com/urfave/cli/v3"
	"os"
	"strings"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var Logger zerolog.Logger
var ShowProgressbar bool

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var fullLogger = zerolog.New(
	zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
).With().Timestamp().Caller().Logger()

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var partialLogger = zerolog.New(
	zerolog.ConsoleWriter{
		Out: os.Stderr,
		FormatLevel: func(i interface{}) string {
			return ""
		},
		FormatTimestamp: func(i interface{}) string {
			return ""
		},
	},
).With().Logger()

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SetLoggerLevel(command *ucli.Command) {
	silent := command.Bool("silent")
	logLevel := strings.ToLower(command.String("log-level"))
	progress := command.Bool("progress")

	switch logLevel {
	case "fatal":
		Logger = fullLogger.Level(zerolog.FatalLevel)
	case "error":
		Logger = fullLogger.Level(zerolog.ErrorLevel)
	case "warn":
		Logger = fullLogger.Level(zerolog.WarnLevel)
	case "info":
		Logger = fullLogger.Level(zerolog.InfoLevel)
	case "debug":
		Logger = fullLogger.Level(zerolog.DebugLevel)
	case "trace":
		Logger = fullLogger.Level(zerolog.TraceLevel)
	default:
		if progress {
			if logLevel != "none" {
				ShowProgressbar = false
				Logger.Warn().Msgf("--log-level and --progress are both set, --progress will be ignored")
			} else {
				ShowProgressbar = true
			}
		} else if !silent {
			Logger = partialLogger.Level(zerolog.InfoLevel)

		}
	}

	Logger.Debug().Msgf("Log level: %s", Logger.GetLevel().String())
}
