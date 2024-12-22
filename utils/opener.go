package utils

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"os"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func OpenOrCreateDatabase(metadataFilePath string) (*os.File, error) {
	_, err := os.Stat(metadataFilePath)
	var file *os.File

	if err != nil {
		file, err = os.Create(metadataFilePath)
		if err != nil {
			var msg = fmt.Sprintf("Error creating metadata db %s: %v", metadataFilePath, err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return nil, nil
		}
	} else {
		file, err = os.Open(metadataFilePath)
		if err != nil {
			var msg = fmt.Sprintf("Error openning metadata db %s: %v", metadataFilePath, err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return nil, nil
		}
	}

	return file, nil
}
