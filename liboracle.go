package liboracle

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"unsafe"
)

// #cgo CFLAGS: -std=c11 -pthread -Wall -Wextra -Wpedantic
// #include "liboracle.h"
import "C"

type Hash [32]byte    // see rcl_hash_t
type Address [20]byte // see rcl_address_t

type Log struct { // see rcl_log_t
	BlockNumber uint64
	Address     Address
	Topics      [4]Hash
}

type Query struct { // see rcl_query_t
	FromBlock uint64
	ToBlock   uint64
	Addresses []Address
	Topics    [][]Hash
}

var queriesPool = sync.Pool{
	New: func() any {
		return new(C.rcl_query_t)
	},
}

type Conn struct {
	db *C.rcl_t
}

func NewDB(data_dir string, ram_limit uint64) (*Conn, error) {
	data_dir_cstr := C.CString(data_dir)
	defer C.free(unsafe.Pointer(data_dir_cstr))

	err := os.MkdirAll(data_dir, 0755)
	if err != nil {
		return nil, err
	}

	db, err := C.rcl_new(data_dir_cstr, C.uint64_t(ram_limit))
	return &Conn{db: db}, err
}

func (conn *Conn) Close() {
	C.rcl_free(conn.db)
}

func (conn *Conn) Query(query *Query) (uint64, error) {
	var pinner runtime.Pinner

	cquery := queriesPool.Get().(*C.rcl_query_t)
	pinner.Pin(cquery)

	cquery.from_block = C.uint64_t(query.FromBlock)
	cquery.to_block = C.uint64_t(query.ToBlock)

	cquery.addresses.len = C.size_t(len(query.Addresses))
	cquery.addresses.data = nil

	if len(query.Addresses) > 0 {
		ptr := &(query.Addresses[0])
		pinner.Pin(ptr)

		cquery.addresses.data = (*C.rcl_address_t)(unsafe.Pointer(ptr))
	}

	if len(query.Topics) > len(cquery.topics) {
		panic("too many topics")
	}

	for i := 0; i < len(cquery.topics); i++ {
		cquery.topics[i].len = 0
		cquery.topics[i].data = nil

		if len(query.Topics) > i && len(query.Topics[i]) > 0 {
			ptr := &(query.Topics[i][0])
			pinner.Pin(ptr)

			cquery.topics[i].len = C.size_t(len(query.Topics[i]))
			cquery.topics[i].data = (*C.rcl_hash_t)(unsafe.Pointer(ptr))
		}
	}

	count, err := C.rcl_query(conn.db, (*C.rcl_query_t)(unsafe.Pointer(cquery)))

	pinner.Unpin()
	queriesPool.Put(cquery)

	return uint64(count), err
}

func (conn *Conn) Insert(logs []Log) error {
	if len(logs) < 1 {
		return nil
	}

	err := C.rcl_insert(
		conn.db,
		C.size_t(len(logs)),
		(*C.rcl_log_t)(unsafe.Pointer(&(logs[0]))),
	)

	if err != 0 {
		return fmt.Errorf("couldn't insert logs, code: %d", int(err))
	}

	return nil
}

func (conn *Conn) GetLogsCount() uint64 {
	return uint64(C.rcl_logs_count(conn.db))
}

func (conn *Conn) GetBlocksCount() uint64 {
	return uint64(C.rcl_blocks_count(conn.db))
}
