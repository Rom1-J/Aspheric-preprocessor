package process

import (
	"encoding/json"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/structs"
	"os"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func OpenDatabase(metadataFilePath string) (*os.File, error) {
	file, err := os.Create(metadataFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Error creating metadata db %s: %v", metadataFilePath, err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil, nil
	}

	return file, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SaveMetadataInfo(file *os.File, metadataInfo structs.MetadataInfoStruct) error {
	jsonData, err := json.Marshal(metadataInfo)
	if err != nil {
		var msg = fmt.Sprintf("Error marshal metadataInfo: %v", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil
	}

	_, err = file.Write(jsonData)
	if err != nil {
		var msg = fmt.Sprintf("Error writing to db: %v", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil

	}

	_, err = file.WriteString("\n")
	if err != nil {
		var msg = fmt.Sprintf("Error writing newline to db: %v", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil

	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SaveMetadata(file *os.File, metadata structs.MetadataStruct) error {
	jsonData, err := json.Marshal(metadata)
	if err != nil {
		var msg = fmt.Sprintf("Error marshal metadata: %v", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil
	}

	_, err = file.Write(jsonData)
	if err != nil {
		var msg = fmt.Sprintf("Error writing to db: %v", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil

	}

	_, err = file.WriteString("\n")
	if err != nil {
		var msg = fmt.Sprintf("Error writing newline to db: %v", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil

	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
