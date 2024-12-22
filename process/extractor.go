package processors

import (
	"bufio"
	"encoding/json"
	"github.com/Rom1-J/preprocessor/constants"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/structs"
	"os"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func Extractor(filePath string) error {
	logger.Logger.Info().Msgf("Extractor starting on: %s", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		logger.Logger.Error().Msgf("Failed to open file: %v", err)
		return nil
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			return
		}
	}(file)

	var metadataList []structs.MetadataStruct
	reader := bufio.NewReader(file)
	offset := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil && len(line) == 0 {
			break
		}

		line = strings.TrimSpace(line)

		fragments := constants.DomainPattern.FindAllString(line, -1)

		metadata := structs.MetadataStruct{
			File:        filePath,
			Offset:      offset,
			Name:        "RandomName",        // Replace with your logic
			Description: "RandomDescription", // Replace with your logic
			Fragments:   fragments,
		}

		metadataList = append(metadataList, metadata)

		offset += len(line) + 1
	}

	outputPath := filePath + ".metadata.json"
	err = saveMetadata(metadataList, outputPath)
	if err != nil {
		logger.Logger.Error().Msgf("failed to save metadata: %v", err)
		return nil
	}

	logger.Logger.Info().Msgf("Metadata saved to: %s\n", outputPath)
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func saveMetadata(metadataList []structs.MetadataStruct, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			logger.Logger.Error().Msgf("Error closing file: %v\n", err)
		}
	}(file)

	for _, metadata := range metadataList {
		jsonData, err := json.Marshal(metadata)
		if err != nil {
			logger.Logger.Error().Msgf("failed to marshal metadata: %v", err)
			return nil
		}

		_, err = file.Write(jsonData)
		if err != nil {
			logger.Logger.Error().Msgf("failed to write to file: %v", err)
			return nil
		}

		_, err = file.WriteString("\n")
		if err != nil {
			logger.Logger.Error().Msgf("failed to write newline to file: %v", err)
			return nil
		}
	}

	return nil
}
