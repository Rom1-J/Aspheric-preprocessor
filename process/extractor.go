package process

import (
	"bufio"
	"fmt"
	"github.com/Rom1-J/preprocessor/constants"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/structs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func Extractor(filePath string) ([]structs.MetadataStruct, error) {
	logger.Logger.Debug().Msgf("Extractor starting on: %s", filePath)

	var (
		metadataList []structs.MetadataStruct

		err error
	)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initializing file reader
	//
	file, err := os.Open(filePath)
	if err != nil {
		var msg = fmt.Sprintf("Failed to open file: %v", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil, fmt.Errorf(msg)
	}

	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			return
		}
	}(file)

	reader := bufio.NewReader(file)
	offset := 0
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Generate metadata collection
	//
	for {
		line, err := reader.ReadString('\n')
		if err != nil && len(line) == 0 {
			break
		}

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Extract fragments
		//
		var fragments []string

		for _, fragmentPattern := range constants.FragmentPatterns {
			fragments = append(fragments, fragmentPattern.FindAllString(line, -1)...)
		}

		if len(fragments) == 0 {
			offset += len(line)
			continue
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Generate metadata struct
		//
		var splitPart = strings.Split(filepath.Ext(filePath), ".part")
		var part int

		if len(splitPart) != 2 {
			part = -1
		} else {
			part, err = strconv.Atoi(splitPart[1])
			if err != nil {
				part = -1
			}
		}

		metadata := structs.MetadataStruct{
			Part:      part,
			Offset:    offset,
			Fragments: fragments,
		}

		metadataList = append(metadataList, metadata)

		offset += len(line)
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	logger.Logger.Debug().Msgf("Extractor finished on: %s", filePath)
	return metadataList, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
