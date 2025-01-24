package process

import (
	"encoding/csv"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/structs"
	"os"
	"strconv"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SaveMetadataInfo(file *os.File, metadataInfo structs.MetadataInfoStruct) error {
	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := []string{
		metadataInfo.Name,
		metadataInfo.Description,
		metadataInfo.Path,
		strconv.Itoa(metadataInfo.Lines),
		strconv.Itoa(metadataInfo.Parts),
	}

	if err := writer.Write(record); err != nil {
		msg := fmt.Sprintf("Error writing to CSV: %v", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SaveMetadata(file *os.File, metadata structs.MetadataStruct) error {
	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, fragment := range metadata.Fragments {
		record := []string{
			fragment,
			strconv.Itoa(metadata.Part),
			strconv.Itoa(metadata.Offset),
		}

		if err := writer.Write(record); err != nil {
			msg := fmt.Sprintf("Error writing to CSV: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
