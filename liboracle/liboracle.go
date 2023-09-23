package liboracle

import (
	"os"
	"sync"
	"unsafe"
)

// #cgo CFLAGS: -std=gnu11 -Wall -Wextra -Wwrite-strings -Wfloat-equal -O3 -march=native
// #include "liboracle.h"
import "C"

type Hash [32]byte    // see db_hash_t
type Address [20]byte // see db_address_t

type Log struct { // see db_log_t
	BlockNumber uint64
	Address     Address
	Topics      [4]Hash
}

type Query struct { // see db_query_t
	FromBlock uint64
	ToBlock   uint64
	Addresses []Address
	Topics    [][]Hash
}

func (q Query) DBQueryT() C.db_query_t {
	internal := C.db_query_t{}

	internal.from_block = C.uint64_t(q.FromBlock)
	internal.to_block = C.uint64_t(q.ToBlock)

	internal.addresses.len = C.size_t(len(q.Addresses))
	internal.addresses.data = nil

	if len(q.Addresses) > 0 {
		bytes := C.size_t(len(q.Addresses)) * C.sizeof_db_address_t
		internal.addresses.data = (*C.db_address_t)(C.malloc(bytes))

		C.memcpy(
			unsafe.Pointer(internal.addresses.data),
			unsafe.Pointer(&(q.Addresses[0])),
			bytes,
		)
	}

	if len(q.Topics) > len(internal.topics) {
		panic("too many topics")
	}

	for i := 0; i < len(internal.topics); i++ {
		internal.topics[i].len = 0
		internal.topics[i].data = nil

		if len(q.Topics) > i && len(q.Topics[i]) > 0 {
			internal.topics[i].len = C.size_t(len(q.Topics[i]))

			bytes := C.size_t(len(q.Topics[i])) * C.sizeof_db_hash_t
			internal.topics[i].data = (*C.db_hash_t)(C.malloc(bytes))

			C.memcpy(
				unsafe.Pointer(internal.topics[i].data),
				unsafe.Pointer(&(q.Topics[i][0])),
				bytes,
			)
		}
	}

	return internal
}

type Conn struct {
	mu sync.RWMutex
	db *C.db_t
}

func NewDB(data_dir string, ram_limit uint64) (*Conn, error) {
	data_dir_cstr := C.CString(data_dir)
	defer C.free(unsafe.Pointer(data_dir_cstr))

	err := os.MkdirAll(data_dir, 0755)
	if err != nil {
		return nil, err
	}

	db, err := C.db_new(data_dir_cstr, C.uint64_t(ram_limit))
	return &Conn{db: db}, err
}

func (conn *Conn) Close() {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	C.db_free(conn.db)
}

func (conn *Conn) Query(query *Query) (uint64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()

	count, err := C.db_query(conn.db, query.DBQueryT())
	return uint64(count), err
}

func (conn *Conn) Insert(logs []Log) error {
	if len(logs) < 1 {
		return nil
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()

	_, err := C.db_insert(
		conn.db,
		C.size_t(len(logs)),
		(*C.db_log_t)(unsafe.Pointer(&(logs[0]))),
	)

	return err
}

func (conn *Conn) GetLastBlock() (uint64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()

	last, err := C.db_current_block(conn.db)
	return uint64(last), err
}

func (conn *Conn) Status() string {
	conn.mu.RLock()
	defer conn.mu.RUnlock()

	size := 1024 // 1KB

	buffer := (*C.char)(C.calloc(C.size_t(size), C.sizeof_char))
	defer C.free(unsafe.Pointer(buffer))

	C.db_status(conn.db, buffer, C.size_t(size))

	return C.GoString(buffer)
}
