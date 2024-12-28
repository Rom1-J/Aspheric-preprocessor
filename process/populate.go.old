package process

import (
	"context"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/structs"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"strings"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func Populate(ctx context.Context, driver neo4j.DriverWithContext, metadata structs.MetadataStruct, metadataInfo structs.MetadataInfoStruct, wg *sync.WaitGroup) {
	defer wg.Done()

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func(session neo4j.SessionWithContext, ctx context.Context) {
		err := session.Close(ctx)
		if err != nil {
			var msg = fmt.Sprintf("Error closing neo4j session: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return
		}
	}(session, ctx)

	for _, fragment := range metadata.Fragments {
		parts := strings.Split(fragment, ".")

		for i := len(parts) - 1; i > 0; i-- {
			parent := parts[i]
			child := parts[i-1]

			_, err := session.ExecuteWrite(
				ctx,
				func(tx neo4j.ManagedTransaction) (any, error) {
					query := `
					MERGE (p:Domain {name: $parent})
					MERGE (c:Domain {name: $child})
					MERGE (p)-[:HAS]->(c)`
					params := map[string]interface{}{
						"parent": parent,
						"child":  child,
					}
					return tx.Run(ctx, query, params)
				},
			)

			if err != nil {
				var msg = fmt.Sprintf("Error creating node relationship for %s -> %s: %v\n", parent, child, err)
				logger.Logger.Error().Msgf(msg)
				fmt.Println(msg)

				return
			}
		}

		leaf := parts[0]
		_, err := session.ExecuteWrite(
			ctx,
			func(tx neo4j.ManagedTransaction) (any, error) {
				query := `
				MATCH (leaf:Domain {name: $leaf})
				SET leaf.part = $part, leaf.offset = $offset`
				params := map[string]interface{}{
					"leaf":        leaf,
					"name":        metadataInfo.Name,
					"description": metadataInfo.Description,
					"path":        metadataInfo.Path,

					"part":   metadata.Part,
					"offset": metadata.Offset,
				}
				return tx.Run(ctx, query, params)
			},
		)

		if err != nil {
			var msg = fmt.Sprintf("Error attaching part and offset to leaf node %s: %v\n", leaf, err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return
		}
	}
}
