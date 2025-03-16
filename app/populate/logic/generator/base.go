package generator

import (
	"github.com/Rom1-J/preprocessor/constants"
	metadataproto "github.com/Rom1-J/preprocessor/proto/metadata"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func CreateChunks(items []*metadataproto.Metadata) <-chan []*metadataproto.Metadata {
	ch := make(chan []*metadataproto.Metadata)

	go func() {
		defer close(ch)
		for i := 0; i < len(items); i += constants.SolrBatchSize {
			end := i + constants.SolrBatchSize
			if end > len(items) {
				end = len(items)
			}
			ch <- items[i:end]
		}
	}()

	return ch
}
