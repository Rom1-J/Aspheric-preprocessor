package generator

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	metadatainfoproto "github.com/Rom1-J/preprocessor/proto"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func ProcessUncompressedFile(id string, date string, basePath string, inputFilePath string) (*metadatainfoproto.MetadataInfo, error) {
	metadata, err := GenerateForFile(id, date, basePath, inputFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to get metadata for %s: %v", inputFilePath, err)
		logger.Logger.Error().Msg(msg)

		return nil, fmt.Errorf(msg)
	}

	return metadata, nil
}
