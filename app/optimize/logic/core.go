package logic

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/app/optimize/logic/generator"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	metadataproto "github.com/Rom1-J/preprocessor/proto/metadata"
	"github.com/jedib0t/go-pretty/v6/progress"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func OptimizeMetadata(
	globalProgress prog.ProgressOptsStruct,
	inputDirectory string,
) error {
	logger.Logger.Trace().Msgf("OptimizeMetadata starting on: %s", inputDirectory)

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
	// Open metadata protobuf
	//
	metadataFilePath := filepath.Join(inputDirectory, "_metadata.pb")

	metadataFile, err := os.Open(metadataFilePath)
	if err != nil {
		logger.Logger.Error().Msgf("Failed to open metadata file %s: %v", metadataFilePath, err)
		return err
	}

	metadataData, err := io.ReadAll(metadataFile)
	if err != nil {
		logger.Logger.Error().Msgf("Failed to read metadata file %s: %v", metadataFilePath, err)
		return err
	}
	if err := metadataFile.Close(); err != nil {
		logger.Logger.Error().Msgf("Failed to close metadata file %s: %v", metadataFilePath, err)
	}

	metadata := &metadataproto.MetadataList{}
	err = proto.Unmarshal(metadataData, metadata)
	if err != nil {
		logger.Logger.Error().Msgf("Failed to unmarshal protobuf data for %s: %v", metadataFilePath, err)
		return err
	}
	metadataData = nil

	tracker.UpdateMessage(fmt.Sprintf("Processing directory %s (%d items)", filepath.Base(inputDirectory), len(metadata.Items)))
	tracker.UpdateTotal(int64(len(metadata.Items)))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Optimize metadata
	//
	for _, item := range metadata.Items {
		var wg sync.WaitGroup
		wg.Add(3)

		go func(m *metadataproto.Metadata) {
			defer wg.Done()
			m.Emails = generator.DeduplicateItems(m.Emails)
		}(item)

		go func(m *metadataproto.Metadata) {
			defer wg.Done()
			m.Ips = generator.DeduplicateItems(m.Ips)
		}(item)

		go func(m *metadataproto.Metadata) {
			defer wg.Done()
			m.Domains = generator.DeduplicateItems(m.Domains)
		}(item)

		wg.Wait()
		logger.Logger.Trace().Msgf("Metadata %s dedupped", item.Id)
		tracker.Increment(1)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Saving optimized metadata
	//
	optimizedMetadataFilePath := filepath.Join(inputDirectory, "_metadata.opti.pb")

	optimizedData, err := proto.Marshal(metadata)
	if err != nil {
		logger.Logger.Error().Msgf("Error encoding optimized metadata %s: %v", optimizedMetadataFilePath, err)
		return err
	}

	err = os.WriteFile(optimizedMetadataFilePath, optimizedData, 0644)
	if err != nil {
		logger.Logger.Error().Msgf("Error creating optimized metadata %s: %v", optimizedMetadataFilePath, err)
		return err
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Closing output descriptor
	//
	metadata = nil
	runtime.GC()

	tracker.MarkAsDone()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return nil
}
