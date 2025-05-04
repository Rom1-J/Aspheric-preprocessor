package generator

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	infoproto "github.com/Rom1-J/preprocessor/proto/info"
	ucli "github.com/urfave/cli/v3"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func ProcessUncompressedFile(id string, command *ucli.Command, inputFilePath string) (*infoproto.MetadataInfo, error) {
	metadata, err := GenerateForFile(id, command, inputFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to get metadata for %s: %v", inputFilePath, err)
		logger.Logger.Error().Msg(msg)

		return nil, fmt.Errorf(msg)
	}

	return metadata, nil
}
