package liboracle

import (
	"fmt"
	"runtime"
	"unsafe"
)

// #cgo CFLAGS: -std=gnu11 -pthread -fno-omit-frame-pointer
// #cgo pkg-config: libcurl libcjson
// #include "liboracle.h"
/*
void _add_address_to_query(rcl_query_t* query, _GoString_* strs) {
	for (size_t i = 0; i < query->alen; ++i)
		query->address[i].encoded = _GoStringPtr(strs[i]);
}
void _add_topics_to_query(rcl_query_t* query, size_t j, _GoString_* strs) {
	for (size_t i = 0; i < query->tlen[j]; ++i)
		query->topics[j][i].encoded = _GoStringPtr(strs[i]);
}
*/
import "C"

type Query struct { // see rcl_query_t
	Limit     *int64
	FromBlock int64
	ToBlock   int64
	Addresses []string
	Topics    [][]string
}

type Conn struct {
	db *C.rcl_t
}

func rcl_error(code C.rcl_result) error {
	if code == C.RCLE_OK {
		return nil
	}

	return fmt.Errorf("liboracle error: " + C.GoString(C.rcl_strerror(code)))
}

func NewDB(data_dir string, ram_limit int64) (*Conn, error) {
	data_dir_cstr := C.CString(data_dir)
	defer C.free(unsafe.Pointer(data_dir_cstr))

	var db *C.rcl_t
	rc := C.rcl_open(data_dir_cstr, C.int64_t(ram_limit), &db)

	return &Conn{db: db}, rcl_error(rc)
}

func (conn *Conn) Close() {
	C.rcl_free(conn.db)
}

func (conn *Conn) UpdateHeight(height int64) error {
	rc := C.rcl_update_height(conn.db, C.int64_t(height))
	return rcl_error(rc)
}

func (conn *Conn) SetUpstream(upstream string) error {
	upstream_cstr := C.CString(upstream)
	defer C.free(unsafe.Pointer(upstream_cstr))

	rc := C.rcl_set_upstream(conn.db, upstream_cstr)
	return rcl_error(rc)
}

func (conn *Conn) Query(query *Query) (int64, error) {
	var pinner runtime.Pinner
	pinner.Pin(query)
	defer pinner.Unpin()

	if len(query.Topics) > len(query.Topics) {
		return 0, fmt.Errorf("too many topics")
	}

	tlen := [C.TOPICS_LENGTH]C.size_t{0}
	for i := 0; i < len(query.Topics); i++ {
		tlen[i] = C.size_t(len(query.Topics[i]))
	}

	var cquery *C.rcl_query_t = nil
	rc := C.rcl_query_alloc(
		&cquery,
		C.size_t(len(query.Addresses)),
		&(tlen[0]),
	)
	if rc != C.RCLE_OK {
		return 0, rcl_error(rc)
	}
	defer C.rcl_query_free(cquery)

	cquery.from = C.int64_t(query.FromBlock)
	cquery.to = C.int64_t(query.ToBlock)

	if query.Limit != nil {
		cquery.limit = C.int64_t(*(query.Limit))
	} else {
		cquery.limit = C.int64_t(-1)
	}

	if len(query.Addresses) > 0 {
		C._add_address_to_query(cquery, &(query.Addresses[0]))
	}

	for i := 0; i < len(query.Topics); i++ {
		if len(query.Topics[i]) > 0 {
			C._add_topics_to_query(cquery, C.size_t(i), &(query.Topics[0][i]))
		}
	}

	var count C.int64_t
	rc = C.rcl_query(conn.db, (*C.rcl_query_t)(unsafe.Pointer(cquery)), &count)
	return int64(count), rcl_error(rc)
}

func (conn *Conn) GetLogsCount() (int64, error) {
	var result C.int64_t
	rc := C.rcl_logs_count(conn.db, &result)
	return int64(result), rcl_error(rc)
}

func (conn *Conn) GetBlocksCount() (int64, error) {
	var result C.int64_t
	rc := C.rcl_blocks_count(conn.db, &result)
	return int64(result), rcl_error(rc)
}
