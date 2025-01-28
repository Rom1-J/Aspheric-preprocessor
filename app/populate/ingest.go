package populate

import (
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"io"
	"net/http"
	"os"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// IngestCSV upload a metadata csv file on Apache Solr
func IngestCSV(metadataPath string, solrURL string, collection string) error {
	url := solrURL +
		"/" + collection +
		"/update" +
		"?fieldnames=id,emails,ips,domains" +
		"&f.emails.split=true&f.emails.separator=|" +
		"&f.ips.split=true&f.ips.separator=|" +
		"&f.domains.split=true&f.domains.separator=|"
	logger.Logger.Debug().Msgf("Ingesting file: %s, collection: %s, url: %s", metadataPath, collection, url)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Retrieving file descriptor
	//
	metadataFile, err := os.Open(metadataPath)
	if err != nil {
		return err
	}

	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			logger.Logger.Error().Msgf("Failed to close file: %v", err)
		}
	}(metadataFile)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Uploading file
	//
	req, err := http.NewRequest("POST", url, metadataFile)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/csv")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			logger.Logger.Error().Msgf("Failed to close body: %v", err)
		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to ingest file: %s, response (%d): %s", metadataPath, resp.StatusCode, string(body))
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return nil
}
