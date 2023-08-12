package netflow

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"

	"netflow-parser/models"
)

const (
	headerOffset = 175
	packetSize   = 74
)

type BinaryFile struct {
	rawPackets    []byte
	packetsAmount int
}

func ReadFile(filename string) (result *BinaryFile, err error) {
	file, err := os.ReadFile(filename)
	if err != nil {
		err = fmt.Errorf("os.ReadFile: %w", err)
		return
	}

	packetsAmount := (len(file) - headerOffset) / packetSize
	footerOffset := headerOffset + packetSize*packetsAmount

	result = &BinaryFile{
		packetsAmount: packetsAmount,
		rawPackets:    file[headerOffset:footerOffset],
	}
	return
}

func (b *BinaryFile) ParseRecords(threadsCount int, filters models.Filters) (records []models.NetFlowRecord, err error) {
	batches := make([][]models.NetFlowRecord, threadsCount)
	packetsPerBatch := b.packetsAmount / threadsCount
	wg := new(sync.WaitGroup)

	for i := range batches {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			startPosition := packetSize * packetsPerBatch * i
			endPosition := packetSize * packetsPerBatch * (i + 1)
			if i == threadsCount-1 {
				endPosition = len(b.rawPackets)
			}

			var batch = make([]models.NetFlowRecord, (endPosition-startPosition)/packetSize)
			err = binary.Read(bytes.NewBuffer(b.rawPackets[startPosition:endPosition]), binary.LittleEndian, &batch)
			if err != nil {
				return
			}
			batches[i] = batch
		}(i)
	}

	wg.Wait()

	for i := range batches {
		for j := range batches[i] {
			r := batches[i][j]
			if filters.Filter(&r) {
				records = append(records, r)
			}
		}
	}
	return
}

func WriteRecordsToFile(records []models.NetFlowRecord) (err error) {
	lines := make([]byte, 0)

	for _, r := range records {
		line := fmt.Sprintf(
			"source: %s, destination: %s, packets: %d, bytes: %d, sport: %d, dport: %d, proto: %d, account_id: %d, tclass: %d, datetime: %d, nf_source: %s\n",
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
		lines = append(lines, line...)
	}

	err = os.WriteFile("result.txt", lines, 0777)
	return nil
}
