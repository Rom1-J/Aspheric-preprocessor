package generator

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	infoproto "github.com/Rom1-J/preprocessor/proto/info"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func ProcessUncompressedFile(id string, date string, inputFilePath string) (*infoproto.MetadataInfo, error) {
	metadata, err := GenerateForFile(id, date, inputFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to get metadata for %s: %v", inputFilePath, err)
		logger.Logger.Error().Msg(msg)

		return nil, fmt.Errorf(msg)
	}

	return metadata, nil
}
