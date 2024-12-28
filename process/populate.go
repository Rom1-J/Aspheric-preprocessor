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
		domains := strings.Split(fragment, ".")

		for i, j := 0, len(domains)-1; i < j; i, j = i+1, j-1 {
			domains[i], domains[j] = domains[j], domains[i]
		}

		var parentNodeID string

		for depth, domain := range domains {
			logger.Logger.Debug().Msgf("[%s] depth=%d, domain=%s", strings.Join(domains, "."), depth, domain)

			nodeID, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
				result, err := tx.Run(ctx, "MERGE (n:Domain {name: $name, depth: $depth}) "+
					"ON CREATE SET n.created = timestamp() "+
					"SET n += {path: $path, part: $part, offset: $offset} "+
					"RETURN id(n)", map[string]any{
					"name":   domain,
					"depth":  depth,
					"path":   metadataInfo.Path,
					"part":   metadata.Part,
					"offset": metadata.Offset,
				})

				if err != nil {
					return nil, err
				}

				nodeID := ""
				if result.Next(ctx) {
					nodeID = fmt.Sprintf("%v", result.Record().Values[0])
				}
				return nodeID, result.Err()
			})

			if err != nil {
				logger.Logger.Error().Msgf("Error checking/creating node for domain %s: %v", domain, err)
				continue
			}

			if parentNodeID != "" {
				_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
					_, err := tx.Run(ctx, "MATCH (child:Domain {id: $childID}), (parent:Domain {id: $parentID}) "+
						"MERGE (parent)-[:HAS]->(child)", map[string]any{
						"childID":  nodeID,
						"parentID": parentNodeID,
					})
					return nil, err
				})

				if err != nil {
					logger.Logger.Error().Msgf("Error creating relationship between parent %s and child %s: %v", parentNodeID, nodeID, err)
				}
			}

			parentNodeID = nodeID.(string)
		}
	}
}
