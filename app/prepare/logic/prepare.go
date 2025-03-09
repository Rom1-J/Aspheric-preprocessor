package logic

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	metadatainfoproto "github.com/Rom1-J/preprocessor/proto"
	"github.com/jedib0t/go-pretty/v6/progress"
	"os"
	"path/filepath"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func PrepareFile(globalProgress prog.ProgressOptsStruct, date string, inputFilePath string, outputDirectoryPath string) (*metadatainfoproto.MetadataInfo, error) {
	logger.Logger.Debug().Msgf("PrepareFile starting on: %s", inputFilePath)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Getting file info
	//
	fileInfo, err := os.Stat(inputFilePath)
	if err != nil {
		logger.Logger.Error().Msgf("Failed to get file info: %v", err)
		return nil, err
	}
	fileSize := fileInfo.Size()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initialize tracker
	//
	tracker := progress.Tracker{
		Message: "Processing file " + filepath.Base(inputFilePath),
		Total:   fileSize,
		Units:   progress.UnitsBytes,
	}
	globalProgress.Pw.AppendTracker(&tracker)
	globalProgress.GlobalTracker.UpdateTotal(globalProgress.GlobalTracker.Total + 1)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Generate metadata
	//
	var metadata *metadatainfoproto.MetadataInfo

	if strings.HasSuffix(inputFilePath, ".compressed") {
		metadata, err = ExtractZstdArchive(date, inputFilePath, outputDirectoryPath)
		if err != nil {
			var msg = fmt.Sprintf("Failed to extract zstd archive: %v", err)
			logger.Logger.Error().Msg(msg)

			globalProgress.Pw.Log(msg)
			tracker.MarkAsErrored()

			return nil, err
		}
	} else {
		metadata, err = ProcessSingleFile(date, inputFilePath, outputDirectoryPath)
		if err != nil {
			var msg = fmt.Sprintf("Failed to process file: %v", err)
			logger.Logger.Error().Msg(msg)

			globalProgress.Pw.Log(msg)
			tracker.MarkAsErrored()

			return nil, err
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	tracker.SetValue(fileSize)
	tracker.MarkAsDone()

	return metadata, nil
}
