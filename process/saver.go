package process

import (
	"encoding/csv"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/structs"
	"os"
	"strconv"
	"strings"
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
		logger.Logger.Error().Msgf("Error writing to CSV: %v", err)
		return nil
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SaveMetadata(file *os.File, metadata structs.MetadataStruct) error {
	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{
		metadata.File,
		strings.Join(metadata.Emails, "|"),
		strings.Join(metadata.IPs, "|"),
		strings.Join(metadata.Domains, "|"),
	}); err != nil {
		logger.Logger.Error().Msgf("Error writing to CSV: %v", err)
		return nil
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
