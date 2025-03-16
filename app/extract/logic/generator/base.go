package generator

import (
	"github.com/Rom1-J/preprocessor/pkg/utils"
	infoproto "github.com/Rom1-J/preprocessor/proto/info"
	"path/filepath"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func RetrieveReadableFilePaths(metadata *infoproto.MetadataInfo) map[string]*infoproto.MetadataInfo {
	fileMap := make(map[string]*infoproto.MetadataInfo)

	var traverse func(node *infoproto.MetadataInfo, currentPath string)
	traverse = func(node *infoproto.MetadataInfo, currentPath string) {
		path := string(node.Path)
		fullPath := filepath.Join(currentPath, path)

		if utils.IsReadable(fullPath) && node.Simhash != 0 {
			fileMap[fullPath] = node
		}

		for _, child := range node.Children {
			traverse(child, fullPath)
		}
	}

	traverse(metadata, "")

	return fileMap
}
