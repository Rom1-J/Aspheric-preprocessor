package logic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Rom1-J/preprocessor/app/populate/logic/generator"
	"github.com/Rom1-J/preprocessor/app/populate/structs"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/pkg/prog"
	metadataproto "github.com/Rom1-J/preprocessor/proto/metadata"
	"github.com/jedib0t/go-pretty/v6/progress"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func ProcessMetadataPb(
	globalProgress prog.ProgressOptsStruct,
	inputMetadataPb string,
	solrOpts structs.SolrOptsStruct,
) error {
	logger.Logger.Trace().Msgf("ProcessMetadataPb starting on: %s", inputMetadataPb)

	url := solrOpts.Address +
		"/" +
		solrOpts.Collection +
		"/update" +
		"?commit=true" +
		"&overwrite=true"

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Initialize tracker
	//
	tracker := progress.Tracker{
		Message: "Processing metadata " + filepath.Base(inputMetadataPb),
		Total:   int64(0),
	}
	globalProgress.Pw.AppendTracker(&tracker)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Open metadata protobuf
	//
	metadataData, err := os.ReadFile(inputMetadataPb)
	if err != nil {
		log.Fatalf("Failed to read metadata file %s: %v", inputMetadataPb, err)
	}

	metadata := &metadataproto.MetadataList{}
	err = proto.Unmarshal(metadataData, metadata)
	if err != nil {
		log.Fatalf("Failed to unmarshal protobuf data for %s: %v", inputMetadataPb, err)
	}

	tracker.UpdateMessage(fmt.Sprintf("Processing directory %s (%d items)", filepath.Base(inputMetadataPb), len(metadata.Items)))
	tracker.UpdateTotal(int64(len(metadata.Items)))
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Load chunks
	//
	for chunk := range generator.CreateChunks(metadata.Items) {
		var docs []structs.SolrDocument

		for _, item := range chunk {
			doc := structs.SolrDocument{
				ID:      item.Id,
				Emails:  generator.ConvertBytesToStrings(item.Emails),
				IPs:     generator.ConvertBytesToStrings(item.Ips),
				Domains: generator.ConvertBytesToStrings(item.Domains),
			}
			docs = append(docs, doc)
		}

		data, err := json.Marshal(docs)
		if err != nil {
			tracker.IncrementWithError(int64(len(docs)))
			logger.Logger.Warn().Msgf("Failed to marshal docs: %v", err)
			continue
		}

		req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
		if err != nil {
			tracker.IncrementWithError(int64(len(docs)))
			logger.Logger.Warn().Msgf("Failed to create request: %v", err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			tracker.IncrementWithError(int64(len(docs)))
			logger.Logger.Warn().Msgf("Failed to send request: %v", err)
			continue
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				tracker.IncrementWithError(int64(len(docs)))
				logger.Logger.Warn().Msgf("Failed to close response body: %v", err)
			}
		}(resp.Body)

		if resp.StatusCode != http.StatusOK {
			tracker.IncrementWithError(int64(len(docs)))
			logger.Logger.Warn().Msgf("Failed to send request: status code %d", resp.StatusCode)
			continue
		}

		tracker.Increment(int64(len(docs)))
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Closing output descriptor
	//
	tracker.MarkAsDone()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	return nil
}
