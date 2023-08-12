package main

import (
	"flag"
	"fmt"
	"log"

	"netflow-parser/models"
	"netflow-parser/pkg/database"
	"netflow-parser/pkg/netflow"
	"netflow-parser/pkg/stopwatch"
)

func main() {
	watch := stopwatch.New("main")

	cfg, err := models.ParseConfigFile("config.json")
	if err != nil {
		log.Fatal("error parsing config file: ", err)
	}
	watch.Mark("config file read")

	db, err := database.Connect(cfg.Database)
	if err != nil {
		log.Fatal("error connecting to database: ", err)
	}
	defer db.Close()
	watch.Mark("database connected")

	accountID := flag.Int("account_id", 0, "Account ID filter")
	tClass := flag.Int("tclass", 0, "TClass filter")
	source := flag.String("source", "", "Source IP filter")
	destination := flag.String("destination", "", "Destination IP filter")
	filename := flag.String("file", "", "Path to the NetFlow binary file")

	flag.Parse()
	watch.Mark("flags parsed")

	var filters models.Filters
	if err = filters.Validate(*accountID, *tClass, *source, *destination); err != nil {
		log.Fatal("error validating flags: ", err)
	}
	watch.Mark("filters validated")

	netflowBinary, err := netflow.ReadFile(*filename)
	if err != nil {
		log.Fatal("error reading binary file: ", err)
	}
	watch.Mark("binary file read")

	records, err := netflowBinary.ParseRecords(cfg.ThreadsCount, filters)
	if err != nil {
		log.Fatal("error parsing binary data: ", err)
	}
	watch.Mark(fmt.Sprintf("records parsed: %d", len(records)))

	if err = db.InsertRecords(records, cfg.Database.InsertBatchSize); err != nil {
		log.Fatal(err)
	}

	// if err = netflow.WriteRecordsToFile(records); err != nil {
	// 	log.Fatal("error writing records to file: ", err)
	// }
	watch.Mark("records inserted")
}
