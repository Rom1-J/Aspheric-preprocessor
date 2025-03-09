package utils

import (
	"github.com/Rom1-J/preprocessor/constants"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

func CanBeChunks(path string) bool {
	stats, err := os.Stat(path)
	if err != nil {
		return false
	}

	if stats.IsDir() {
		return false
	}

	ext := filepath.Ext(path)

	return stats.Size() > constants.ChunkSize && slices.Contains(constants.TextFilesExtensions, strings.ToLower(ext))
}
