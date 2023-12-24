package liboracle

import (
	"fmt"
	"os"
	"runtime"
	"unsafe"
)

// #cgo CFLAGS: -std=gnu11 -pthread
// #cgo pkg-config: libcurl jansson
// #include "liboracle.h"
/*
void _add_address_to_query(rcl_query_t* query, _GoString_* strs) {
	for (size_t i = 0; i < query->alen; ++i) {
		query->address[i].encoded = _GoStringPtr(strs[i]);
	}
}

void _add_topics_to_query(rcl_query_t* query, size_t j, _GoString_* strs) {
	for (size_t i = 0; i < query->tlen[j]; ++i) {
		query->topics[j][i].encoded = _GoStringPtr(strs[i]);
	}
}
*/
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
	Addresses []string
	Topics    [][]string
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

	var db *C.rcl_t
	rc := C.rcl_open(data_dir_cstr, C.uint64_t(ram_limit), &db)
	if rc != C.RCL_SUCCESS {
		return nil, fmt.Errorf("liboracle: failed connection, code: %d", int(rc))
	}

	return &Conn{db: db}, err
}

func (conn *Conn) Close() {
	C.rcl_free(conn.db)
}

func (conn *Conn) UpdateHeight(height uint64) error {
	rc := C.rcl_update_height(conn.db, C.uint64_t(height))
	if rc != C.RCL_SUCCESS {
		return fmt.Errorf("liboracle: failed rcl_update_height, code: %d", int(rc))
	}

	return nil
}

func (conn *Conn) SetUpstream(upstream string) error {
	upstream_cstr := C.CString(upstream)
	defer C.free(unsafe.Pointer(upstream_cstr))

	rc := C.rcl_set_upstream(conn.db, upstream_cstr)
	if rc != C.RCL_SUCCESS {
		return fmt.Errorf("liboracle: failed rcl_update_height, code: %d", int(rc))
	}

	return nil
}

func (conn *Conn) Query(query *Query) (uint64, error) {
	var pinner runtime.Pinner
	pinner.Pin(query)
	defer pinner.Unpin()

	tlen := [4]C.size_t{0}

	if len(query.Topics) > len(query.Topics) {
		return 0, fmt.Errorf("too many topics")
	}

	for i := 0; i < len(query.Topics); i++ {
		tlen[i] = C.size_t(len(query.Topics[i]))
	}

	var cquery *C.rcl_query_t
	rc := C.rcl_query_new(
		&cquery,
		C.size_t(len(query.Addresses)),
		&(tlen[0]),
	)
	if rc != C.RCL_SUCCESS {
		return 0, fmt.Errorf("liboracle: query failed, code: %d", int(rc))
	}
	defer C.rcl_query_free(cquery)

	cquery.from = C.uint64_t(query.FromBlock)
	cquery.to = C.uint64_t(query.ToBlock)

	if len(query.Addresses) > 0 {
		C._add_address_to_query(cquery, &(query.Addresses[0]))
	}

	for i := 0; i < len(query.Topics); i++ {
		if len(query.Topics[i]) > 0 {
			C._add_topics_to_query(cquery, C.size_t(i), &(query.Topics[0][i]))
		}
	}

	var count C.uint64_t
	rc = C.rcl_query(conn.db, (*C.rcl_query_t)(unsafe.Pointer(cquery)), &count)
	if rc != C.RCL_SUCCESS {
		return 0, fmt.Errorf("liboracle: query failed, code: %d", int(rc))
	}

	return uint64(count), nil
}

func (conn *Conn) Insert(logs []Log) error {
	if len(logs) < 1 {
		return nil
	}

	rc := C.rcl_insert(
		conn.db,
		C.size_t(len(logs)),
		(*C.rcl_log_t)(unsafe.Pointer(&(logs[0]))),
	)

	if rc != C.RCL_SUCCESS {
		return fmt.Errorf("couldn't insert logs, code: %d", int(rc))
	}

	return nil
}

func (conn *Conn) GetLogsCount() (uint64, error) {
	var result C.uint64_t

	if rc := C.rcl_logs_count(conn.db, &result); rc != C.RCL_SUCCESS {
		return 0, fmt.Errorf("couldn't get logs count, code: %d", int(rc))
	}

	return uint64(result), nil
}

func (conn *Conn) GetBlocksCount() (uint64, error) {
	var result C.uint64_t

	if rc := C.rcl_blocks_count(conn.db, &result); rc != C.RCL_SUCCESS {
		return 0, fmt.Errorf("couldn't get blocks count, code: %d", int(rc))
	}

	return uint64(result), nil
}
