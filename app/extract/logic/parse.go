package logic

import (
	"bufio"
	"fmt"
	"github.com/Rom1-J/preprocessor/app/extract/structs"
	"github.com/Rom1-J/preprocessor/constants"
	"github.com/Rom1-J/preprocessor/logger"
	"os"
	"path/filepath"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func Parse(filePath string) (<-chan structs.MetadataStruct, error) {
	logger.Logger.Trace().Msgf("Extract starting on: %s", filePath)

	metadataChan := make(chan structs.MetadataStruct)

	go func() {
		defer close(metadataChan)

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Initializing fragments
		//
		var (
			emails  []string
			ips     []string
			domains []string
		)
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Initializing file reader
		//
		file, err := os.Open(filePath)
		if err != nil {
			logger.Logger.Error().Msgf("Failed to open file: %v", err)

			return
		}

		defer func(file *os.File) {
			if err := file.Close(); err != nil {
				return
			}
		}(file)

		reader := bufio.NewReader(file)
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Generate metadata collection
		//
		for {
			line, err := reader.ReadString('\n')
			if err != nil && len(line) == 0 {
				break
			}

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Extract fragments
			//
			emails = append(emails, constants.EmailPattern.FindAllString(line, -1)...)
			ips = append(ips, constants.IpPattern.FindAllString(line, -1)...)
			domains = append(domains, constants.DomainPattern.FindAllString(line, -1)...)
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Returning metadata
		//
		metadataChan <- structs.MetadataStruct{
			File:    fmt.Sprintf("%s/%s", filepath.Base(filepath.Dir(filePath)), filepath.Base(filePath)),
			Emails:  emails,
			IPs:     ips,
			Domains: domains,
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		logger.Logger.Trace().Msgf("Extract finished on: %s", filePath)
	}()

	return metadataChan, nil
}
