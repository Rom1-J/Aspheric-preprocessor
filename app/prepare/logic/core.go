package logic

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/app/prepare/logic/generator"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	infoproto "github.com/Rom1-J/preprocessor/proto/info"
	"github.com/jedib0t/go-pretty/v6/progress"
	ucli "github.com/urfave/cli/v3"
	"os"
	"path/filepath"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func PrepareFile(
	globalProgress prog.ProgressOptsStruct,
	id string,
	command *ucli.Command,
	inputFilePath string,
	outputDirectoryPath string,
) (*infoproto.MetadataInfo, error) {
	logger.Logger.Trace().Msgf("PrepareFile starting on: %s", inputFilePath)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Get file info
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
	// Copy file to output directory
	//
	dataDirectoryPath := filepath.Join(outputDirectoryPath, "data")
	copiedFilePath := filepath.Join(dataDirectoryPath, fileInfo.Name())

	data, err := os.ReadFile(inputFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to read file %s: %v", inputFilePath, err)
		logger.Logger.Error().Msg(msg)

		return nil, err
	}
	err = os.WriteFile(copiedFilePath, data, 0644)
	if err != nil {
		var msg = fmt.Sprintf("Failed to write file %s: %v", copiedFilePath, err)
		logger.Logger.Error().Msg(msg)

		return nil, err
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Generate metadata info
	//
	var metadataInfo *infoproto.MetadataInfo

	if strings.HasSuffix(copiedFilePath, ".compressed") {
		metadataInfo, err = generator.ProcessCompressedFile(id, command, copiedFilePath)
		if err != nil {
			var msg = fmt.Sprintf("Failed to process compressed file: %v", err)
			logger.Logger.Error().Msg(msg)

			globalProgress.Pw.Log(msg)
			tracker.MarkAsErrored()

			return nil, err
		}
	} else {
		metadataInfo, err = generator.ProcessUncompressedFile(id, command, copiedFilePath)
		if err != nil {
			var msg = fmt.Sprintf("Failed to process text file: %v", err)
			logger.Logger.Error().Msg(msg)

			globalProgress.Pw.Log(msg)
			tracker.MarkAsErrored()

			return nil, err
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	//// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	////
	//// Compress output data directory
	////
	//compressedDataDirectoryPath, err := archive.CompressZstdArchive(dataDirectoryPath)
	//if err != nil {
	//	var msg = fmt.Sprintf("Failed to compress archive archive from %s to %s: %v", dataDirectoryPath, compressedDataDirectoryPath, err)
	//	logger.Logger.Warn().Msg(msg)
	//} else {
	//	if err := os.RemoveAll(dataDirectoryPath); err != nil {
	//		var msg = fmt.Sprintf("Failed to remove file %s: %v", dataDirectoryPath, err)
	//		logger.Logger.Warn().Msg(msg)
	//	}
	//}
	//// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	tracker.SetValue(fileSize)
	tracker.MarkAsDone()

	return metadataInfo, nil
}
