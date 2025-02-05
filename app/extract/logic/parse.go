package logic

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/Rom1-J/preprocessor/app/extract/structs"
	"github.com/Rom1-J/preprocessor/constants"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/urfave/cli/v3"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func Parse(filePath string, command *cli.Command) (<-chan structs.MetadataStruct, error) {
	logger.Logger.Debug().Msgf("Extract starting on: %s", filePath)

	metadataChan := make(chan structs.MetadataStruct)

	go func() {
		defer close(metadataChan)

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Initializing fragments
		//
		var (
			emails       []string
			ips          []string
			domains      []string
			phonenumbers []string
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
			modules := command.StringSlice("module")
			if slices.Contains(modules, "email") {
				logger.Logger.Debug().Msgf("Use email module")
				emails = append(emails, constants.EmailPattern.FindAllString(line, -1)...)
			}

			if slices.Contains(modules, "ip") {
				logger.Logger.Debug().Msgf("Use ip module")
				ips = append(ips, constants.IpPattern.FindAllString(line, -1)...)
			}

			if slices.Contains(modules, "domain") {
				logger.Logger.Debug().Msgf("Use domain module")
				domains = append(domains, constants.DomainPattern.FindAllString(line, -1)...)
			}

			if slices.Contains(modules, "phonenumber") {
				logger.Logger.Debug().Msgf("Use phonenumber module")
				phonenumbers = append(domains, constants.PhonePattern.FindAllString(line, -1)...)
			}

			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Returning metadata
		//
		metadataChan <- structs.MetadataStruct{
			File:         fmt.Sprintf("%s/%s", filepath.Base(filepath.Dir(filePath)), filepath.Base(filePath)),
			Emails:       emails,
			IPs:          ips,
			Domains:      domains,
			PhoneNumbers: phonenumbers,
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		logger.Logger.Debug().Msgf("Extract finished on: %s", filePath)
	}()

	return metadataChan, nil
}
