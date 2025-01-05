package process

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/structs"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func Populate(client *elasticsearch.Client, bucketUUID string, filesPath []string) (esutil.BulkIndexerStats, error) {
	var (
		documents []*structs.DocumentStruct

		res     *esapi.Response
		biStats esutil.BulkIndexerStats
		err     error
	)

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initializing index metadata
	//
	indexName := fmt.Sprintf("bucket-%s", bucketUUID)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Create BulkIndexer
	//
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         indexName,        // The default index name
		Client:        client,           // The Elasticsearch client
		NumWorkers:    4,                // The number of worker goroutines
		FlushBytes:    int(5e+6),        // The flush threshold in bytes
		FlushInterval: 30 * time.Second, // The periodic flush interval
	})
	if err != nil {
		var msg = fmt.Sprintf("Error creating the bulk indexer of %s: %v", indexName, err)
		logger.Logger.Fatal().Msgf(msg)

		return biStats, fmt.Errorf(msg)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Generate document collection
	//
	for _, filePath := range filesPath {
		fileName := filepath.Base(filePath)
		partStr := strings.TrimPrefix(strings.TrimSuffix(fileName, filepath.Ext(fileName)), "part")
		part, err := strconv.Atoi(partStr)
		if err != nil {
			var msg = fmt.Sprintf("Error retrieving part number of %s: %v", filePath, err)
			logger.Logger.Fatal().Msgf(msg)

			return biStats, fmt.Errorf(msg)
		}

		file, err := os.Open(filePath)
		if err != nil {
			var msg = fmt.Sprintf("Error opening file %s: %v", filePath, err)
			logger.Logger.Error().Msgf(msg)

			return biStats, fmt.Errorf(msg)
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				var msg = fmt.Sprintf("Error closing file %s: %v", filePath, err)
				logger.Logger.Error().Msgf(msg)

				return
			}
		}(file)

		reader := csv.NewReader(file)

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}

			if err != nil {
				var msg = fmt.Sprintf("Error reading CSV record for file %s: %v", filePath, err)
				logger.Logger.Error().Msgf(msg)

				return biStats, fmt.Errorf(msg)
			}

			if len(record) < 2 {
				var msg = fmt.Sprintf("Invalid CSV record for file %s: %v", filePath, err)
				logger.Logger.Error().Msgf(msg)

				return biStats, fmt.Errorf(msg)
			}

			fragment := record[0]
			offset, err := strconv.Atoi(record[1])
			if err != nil {
				var msg = fmt.Sprintf("Error converting offset to integer for file %s: %v", filePath, err)
				logger.Logger.Error().Msgf(msg)

				return biStats, fmt.Errorf(msg)
			}

			fragmentParts := strings.Split(fragment, ".")
			tld := fragmentParts[len(fragmentParts)-1]

			documents = append(
				documents,
				&structs.DocumentStruct{
					Part:     part,
					Offset:   offset,
					Fragment: fragment,
					TLD:      tld,
				},
			)
		}
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Re-creating index
	//
	if res, err = client.Indices.Delete([]string{indexName}, client.Indices.Delete.WithIgnoreUnavailable(true)); err != nil {
		var msg = fmt.Sprintf("Error deleting index %s: %v", indexName, err)
		logger.Logger.Fatal().Msgf(msg)

		return biStats, fmt.Errorf(msg)
	}
	if res.IsError() {
		var msg = fmt.Sprintf("Error deleting index %s: %s", indexName, res.String())
		logger.Logger.Fatal().Msgf(msg)

		return biStats, fmt.Errorf(msg)
	}
	if err := res.Body.Close(); err != nil {
		var msg = fmt.Sprintf("Error closing delete index %s: %v", indexName, err)
		logger.Logger.Error().Msgf(msg)

		return biStats, fmt.Errorf(msg)
	}

	// Create index
	//
	res, err = client.Indices.Create(indexName)
	if err != nil {
		var msg = fmt.Sprintf("Error creating index %s: %v", indexName, err)
		logger.Logger.Fatal().Msgf(msg)

		return biStats, fmt.Errorf(msg)
	}
	if res.IsError() {
		var msg = fmt.Sprintf("Error creating index %s: %s", indexName, res.String())
		logger.Logger.Fatal().Msgf(msg)

		return biStats, fmt.Errorf(msg)
	}
	if err := res.Body.Close(); err != nil {
		var msg = fmt.Sprintf("Error closing create index %s: %v", indexName, err)
		logger.Logger.Error().Msgf(msg)

		return biStats, fmt.Errorf(msg)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	for _, document := range documents {
		data, err := json.Marshal(document)
		if err != nil {
			var msg = fmt.Sprintf("Cannot encode document %d: %s", document.Fragment, err)
			logger.Logger.Error().Msgf(msg)

			continue
		}

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Add document to BulkIndexer
		//
		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "index",
				Body:   bytes.NewReader(data),
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						logger.Logger.Error().Msgf("ERROR: %s", err)
					} else {
						logger.Logger.Error().Msgf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			var msg = fmt.Sprintf("Unexpected error: %s", err)
			logger.Logger.Error().Msgf(msg)

			continue
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	}

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Close BulkIndexer
	//
	if err := bi.Close(context.Background()); err != nil {
		var msg = fmt.Sprintf("Error closing the bulk indexer of %s: %v", indexName, err)
		logger.Logger.Error().Msgf(msg)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	biStats = bi.Stats()

	return biStats, nil
}
