package logic

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/app/extract/logic/generator"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	infoproto "github.com/Rom1-J/preprocessor/proto/info"
	metadataproto "github.com/Rom1-J/preprocessor/proto/metadata"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/urfave/cli/v3"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func ProcessDirectory(
	globalProgress prog.ProgressOptsStruct,
	wg *sync.WaitGroup,
	semaphore chan struct{},
	inputDirectory string,
	command *cli.Command,
) (*metadataproto.MetadataList, error) {
	logger.Logger.Trace().Msgf("ProcessDirectory starting on: %s", inputDirectory)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Skip if _metadata.pb exists
	//
	metadataFilePath := filepath.Join(inputDirectory, "_metadata.pb")

	if _, err := os.Stat(metadataFilePath); err == nil {
		if !command.Bool("overwrite") {
			message := fmt.Sprintf(
				"Skipping directory '%s', use --overwrite to ignore existing _metadata.pb",
				inputDirectory,
			)

			return nil, fmt.Errorf(message)
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initialize tracker
	//
	tracker := progress.Tracker{
		Message: "Processing directory " + filepath.Base(inputDirectory),
		Total:   int64(0),
	}
	globalProgress.Pw.AppendTracker(&tracker)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Open metadata info protobuf
	//
	metadataInfoFilePath := filepath.Join(inputDirectory, "_info.pb")

	metadataInfoData, err := os.ReadFile(metadataInfoFilePath)
	if err != nil {
		log.Fatalf("Failed to read metadata info file %s: %v", metadataInfoFilePath, err)
	}

	metadataInfo := &infoproto.MetadataInfo{}
	err = proto.Unmarshal(metadataInfoData, metadataInfo)
	if err != nil {
		log.Fatalf("Failed to unmarshal protobuf data for %s: %v", metadataInfoFilePath, err)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Load paths
	//
	paths := generator.RetrieveReadableFilePaths(metadataInfo)

	if len(paths) == 0 {
		logger.Logger.Warn().Msgf("No paths found for %s", metadataInfoFilePath)
	}

	tracker.UpdateMessage(fmt.Sprintf("Processing directory %s (%d files)", filepath.Base(inputDirectory), len(paths)))
	tracker.UpdateTotal(int64(len(paths)))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	metadataList := &metadataproto.MetadataList{}

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Process paths
	//
	for path, metadataInfo := range paths {
		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Formalize and check path existence
		//
		var dataFilePath string
		parts := strings.Split(path, "/")

		if !strings.HasSuffix(parts[0], ".chunked") || len(metadataInfo.Children) > 0 {
			dataFilePath = filepath.Join(
				inputDirectory,
				"data",
				filepath.Join(parts[1:]...),
			)
		} else {
			dataFilePath = filepath.Join(
				inputDirectory,
				"data",
				filepath.Join(parts...),
			)
		}

		if _, err := os.Stat(dataFilePath); os.IsNotExist(err) {
			logger.Logger.Warn().Msgf("Skipping path '%s', file does not exist: %s", dataFilePath, path)
			continue
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		logger.Logger.Trace().Msgf("Locking slot for: %s", dataFilePath)
		semaphore <- struct{}{}
		wg.Add(1)

		go func(p string, i *infoproto.MetadataInfo) {
			defer func() {
				tracker.Increment(1)

				logger.Logger.Trace().Msgf("Releasing slot for: %s", p)
				<-semaphore
				wg.Done()
			}()

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Extracting metadata from paths
			//
			metadata, err := generator.Extract(p, i)
			if err != nil {
				logger.Logger.Error().Msgf("Error starting extractor for file %s: %v", p, err)
				return
			}

			metadataList.Items = append(metadataList.Items, metadata)
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}(dataFilePath, metadataInfo)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Closing output descriptor
	//
	wg.Wait()

	tracker.MarkAsDone()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return metadataList, nil
}
