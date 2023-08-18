package db

import (
	"math/big"
	"sync"
	"unsafe"
	"os"
)

// #include "db.hh"
import "C"

type Address [20]byte
type Hash [32]byte

type Log struct {
	BlockNumber uint64
	Address     Address
	Topics      [4]Hash
}

type Conn struct {
	mu sync.RWMutex
	db *C.db_t
}

func New(dsn string) (*Conn, error) {
	dsn_cstr := C.CString(dsn)
	defer C.free(unsafe.Pointer(dsn_cstr))

	err := os.MkdirAll(dsn, 0750)
	if err != nil {
		return nil, err
	}

	db, err := C.db_new(dsn_cstr)
	return &Conn{db: db}, err
}

func (conn *Conn) Close() {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	C.db_close(conn.db)
}

func (conn *Conn) GetLogsCount(
	fromBlock, toBlock big.Int,
	addresses []Address,
	topics [][]Hash,
) (uint64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()

	var addr *C.db_address_t = nil
	if len(addresses) > 0 {
		addr = (*C.db_address_t)(unsafe.Pointer(&(addresses[0])))
	}

	count, err := C.db_query(
		conn.db,
		C.uint64_t(fromBlock.Uint64()), C.uint64_t(toBlock.Uint64()),
		C.size_t(len(addresses)), addr,
	)
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

	last, err := C.db_last_block(conn.db)
	return uint64(last), err
}
