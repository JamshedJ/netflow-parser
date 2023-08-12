package database

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"netflow-parser/models"
	"netflow-parser/pkg/stopwatch"
)

type Database struct {
	db *sql.DB
}

func Connect(cfg models.ConfigDatabase) (d Database, err error) {
	conf := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.DbName)
	d.db, err = sql.Open("mysql", conf)
	if err != nil {
		err = fmt.Errorf("sql.Open: %w", err)
		return
	}

	createTable := `
		CREATE TABLE IF NOT EXISTS netflow (
			id BIGINT AUTO_INCREMENT,
			source VARCHAR(15),
			destination VARCHAR(15),
			packets BIGINT,
			bytes BIGINT,
			sport INT,
			dport INT,
			proto INT,
			account_id BIGINT,
			tclass BIGINT,
			date_time BIGINT,
			nf_source VARCHAR(15),
			PRIMARY KEY (id)
		);`
	if _, err = d.db.Exec(createTable); err != nil {
		err = fmt.Errorf("failed to create table: %w", err)
	}
	return
}

func (d *Database) Close() {
	if err := d.db.Close(); err != nil {
		log.Fatal("error closing database: ", err)
	}
}

func (d *Database) InsertRecords(records []models.NetFlowRecord, batchSize int) (err error) {
	watch := stopwatch.New("db")

	sqlQuery := `INSERT INTO netflow (
			source,
			destination,
			packets,
			bytes,
			sport,
			dport,
			proto,
			account_id,
			tclass,
			date_time,
			nf_source
		) VALUES `

	placeholder := "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	for startPos := 0; startPos < len(records); startPos += batchSize {
		endPos := startPos + batchSize
		if endPos > len(records) {
			endPos = len(records)
		}

		placeholders := make([]string, 0)
		arguments := make([]interface{}, 0)

		for _, r := range records[startPos:endPos] {
			placeholders = append(placeholders, placeholder)
			arguments = append(arguments,
				net.IP{r.Source[3], r.Source[2], r.Source[1], r.Source[0]}.String(),
				net.IP{r.Destination[3], r.Destination[2], r.Destination[1], r.Destination[0]}.String(),
				r.Packets,
				r.Bytes,
				r.Sport,
				r.Dport,
				r.Proto,
				r.AccountID,
				r.TClass,
				r.DateTime,
				net.IP{r.NfSource[3], r.NfSource[2], r.NfSource[1], r.NfSource[0]}.String(),
			)
		}

		batchQuery := sqlQuery + strings.Join(placeholders, ",") + ";"
		if _, err = d.db.Exec(batchQuery, arguments...); err != nil {
			return
		}
		watch.Mark(fmt.Sprintf("inserted: %d", endPos))
	}

	watch.Mark("records inserted")
	return
}
