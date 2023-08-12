package models

import (
	"errors"
	"math"
	"net"
)

type NetFlowRecord struct {
	_           uint8 // deviceID
	Source      [4]byte
	Destination [4]byte
	_           uint32 // nexthop
	_           uint16 // iface
	_           uint16 // oface
	Packets     uint32
	Bytes       uint32
	_           uint32 // startTime
	_           uint32 // endTime
	Sport       uint16
	Dport       uint16
	_           uint8 // tcp_flags
	Proto       uint8
	_           uint8  // tos
	_           uint32 // src_as
	_           uint32 // dst_as
	_           uint8  // src_mask
	_           uint8  // dst_mask
	_           uint32 // slink_id
	AccountID   uint32
	_           uint32 // billingIP
	TClass      uint32
	DateTime    uint32
	NfSource    [4]byte
}

type Filters struct {
	useAccountID   bool
	useTClass      bool
	useSource      bool
	useDestination bool

	acc uint32
	tcl uint32
	src [4]byte
	dst [4]byte
}

func (f *Filters) Validate(accountID, tClass int, source, destination string) error {
	if accountID < 0 || accountID > math.MaxUint32 {
		return errors.New("invalid account_id")
	}
	if accountID != 0 {
		f.acc = uint32(accountID)
		f.useAccountID = true
	}
	if tClass < 0 || tClass > math.MaxUint32 {
		return errors.New("invalid account_id")
	}
	if tClass != 0 {
		f.tcl = uint32(tClass)
		f.useTClass = true
	}

	if source != "" {
		ip := net.ParseIP(source).To4()
		if ip == nil {
			return errors.New("invalid source")
		}
		f.src = [4]byte{ip[3], ip[2], ip[1], ip[0]}
		f.useSource = true
	}
	if destination != "" {
		ip := net.ParseIP(destination).To4()
		if ip == nil {
			return errors.New("invalid destination")
		}
		f.dst = [4]byte{ip[3], ip[2], ip[1], ip[0]}
		f.useDestination = true
	}
	return nil
}

func (f *Filters) Filter(r *NetFlowRecord) bool {
	if f.useAccountID && f.acc != r.AccountID {
		return false
	}
	if f.useTClass && f.tcl != r.TClass {
		return false
	}
	if f.useSource && f.src != r.Source {
		return false
	}
	if f.useDestination && f.dst != r.Destination {
		return false
	}
	return true
}
