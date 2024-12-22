package process

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Rom1-J/preprocessor/logger"
	"github.com/Rom1-J/preprocessor/structs"
	_ "github.com/mattn/go-sqlite3"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func InitDatabase(metadataFilePath string) error {
	db, err := OpenDatabase(metadataFilePath)
	if err != nil {
		return err
	}
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			var msg = fmt.Sprintf("Error closing metadata db %s: %v", metadataFilePath, err.Error())
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return
		}
	}(db)

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS metadata (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		file TEXT,
		offset INTEGER,
		name TEXT DEFAULT '',
		description TEXT DEFAULT '',
		fragments JSON DEFAULT NULL
	);
	`

	stmt, err := db.Prepare(createTableQuery)
	if err != nil {
		var msg = fmt.Sprintf("Error preparing create table query: %s", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil
	}
	defer func(stmt *sql.Stmt) {
		if err := stmt.Close(); err != nil {
			var msg = fmt.Sprintf("Error closing create table query: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return
		}
	}(stmt)

	_, err = stmt.Exec()
	if err != nil {
		var msg = fmt.Sprintf("Error executing create table query: %s", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func OpenDatabase(metadataFilePath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", metadataFilePath)
	if err != nil {
		var msg = fmt.Sprintf("Error creating metadata db %s: %v", metadataFilePath, err.Error())
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil, nil
	}

	return db, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SaveMetadata(db *sql.DB, metadata structs.MetadataStruct) error {
	stmt, err := db.Prepare(`INSERT INTO metadata (file, offset, name, description, fragments) values(?, ?, ?, ?, json(?))`)
	if err != nil {
		var msg = fmt.Sprintf("Error preparing insert metadata query: %s", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil
	}
	defer func(stmt *sql.Stmt) {
		if err := stmt.Close(); err != nil {
			var msg = fmt.Sprintf("Error closing insert metadata query: %v", err)
			logger.Logger.Error().Msgf(msg)
			fmt.Println(msg)

			return
		}
	}(stmt)

	fragments, err := json.Marshal(metadata.Fragments)
	if err != nil {
		fmt.Println("Error converting to JSON:", err)
		return nil
	}

	_, err = stmt.Exec(metadata.File, metadata.Offset, metadata.Name, metadata.Description, fragments)
	if err != nil {
		var msg = fmt.Sprintf("Error executing insert metadata query: %s", err)
		logger.Logger.Error().Msgf(msg)
		fmt.Println(msg)

		return nil
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
