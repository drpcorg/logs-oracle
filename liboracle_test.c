#include <stdlib.h>
#include <unistd.h>

#include <criterion/criterion.h>

#include "liboracle.h"

db_t* make_db(void) {
  char tmpl[] = "/tmp/tmpdir.XXXXXX";
  char* dir_name = mkdtemp(tmpl);

  if (dir_name == NULL) {
    perror("mkdtemp failed: ");
    return NULL;
  }

  return db_new(dir_name, 0);
}

Test(oracle, db_new) {
  db_t* db = make_db();
  cr_assert(db != NULL);

  db_free(db);
}

/*
package liboracle

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// ----------------------
// Section 0: utilities
// ----------------------

func HexToBytes(str string) []byte {
	if len(str) >= 2 &&
		str[0] == '0' &&
		(str[1] == 'x' || str[1] == 'X') {
		str = str[2:]
	}

	if len(str)%2 == 1 {
		str = "0" + str
	}

	h, _ := hex.DecodeString(str)
	return h
}

func HexToHash(hex string) Hash {
	hash := Hash{}
	copy(hash[:], HexToBytes(hex))
	return hash
}

func HexToAddress(hex string) Address {
	address := Address{}
	copy(address[:], HexToBytes(hex))
	return address
}

func makeDB(t *testing.T) *Conn {
	dir := t.TempDir()

	// create DB
	conn, err := NewDB(dir, 0)
	require.NoError(t, err, "new db")

	return conn
}

// ----------------------------
// Section 1: test data suite
// ----------------------------

var addresses = [...]Address{
	HexToAddress("0xe4e50b96f70aab13a2d7e654d07d7d4173319653"),
	HexToAddress("0xe53ec727dbdeb9e2d5456c3be40cff031ab40a55"),
	HexToAddress("0xe8da2e3d904e279220a86634aafa4d3be43c89d9"),
	HexToAddress("0xe921401d18ed1ea4d64169d1576c32f9a7439694"),
	HexToAddress("0xe9a1a323b4c8fd5ce6842edaa0cd8af943cbdf22"),
	HexToAddress("0xeae6fd7d8c1740f3f1b03e9a5c35793cd260b9a6"),
	HexToAddress("0xf151980e7a781481709e8195744bf2399fb3cba4"),
	HexToAddress("0xf203ca1769ca8e9e8fe1da9d147db68b6c919817"),
	HexToAddress("0xf411903cbc70a74d22900a5de66a2dda66507255"),
	HexToAddress("0xf57e7e7c23978c3caec3c3548e3d615c346e79ff"),
}

var topics = [...]Hash{
	HexToHash("0xa8dc30b66c6d4a8aac3d15925bfca09e42cac4a00c50f9949154b045088e2ac2"),
	HexToHash("0xb3d987963d01b2f68493b4bdb130988f157ea43070d4ad840fee0466ed9370d9"),
	HexToHash("0xb84b9c38fdca745491d1f429e19a8e2f07a19bc7f6dffb0003c1abb7cb873509"),
	HexToHash("0xbb123b5c06d5408bbea3c4fef481578175cfb432e3b482c6186f02ed9086585b"),
	HexToHash("0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b"),
	HexToHash("0xbd5c436f8c83379009c1962310b8347e561d1900906d3fe4075b1596f8955f88"),
	HexToHash("0xbeee1e6e7fe307ddcf84b0a16137a4430ad5e2480fc4f4a8e250ab56ccd7630d"),
	HexToHash("0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"),
	HexToHash("0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9"),
	HexToHash("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"),
}

var suite = []Log{
	Log{BlockNumber: 0, Address: addresses[2], Topics: [4]Hash{Hash{}, Hash{}, Hash{}, Hash{}}},
	Log{BlockNumber: 0, Address: addresses[2], Topics: [4]Hash{Hash{}, Hash{}, Hash{}, Hash{}}},
	Log{BlockNumber: 0, Address: addresses[6], Topics: [4]Hash{topics[9], topics[0], topics[2], topics[2]}},

	Log{BlockNumber: 3, Address: addresses[8], Topics: [4]Hash{Hash{}, Hash{}, Hash{}, Hash{}}},
	Log{BlockNumber: 3, Address: addresses[2], Topics: [4]Hash{topics[9], Hash{}, Hash{}, Hash{}}},
	Log{BlockNumber: 3, Address: addresses[8], Topics: [4]Hash{topics[5], topics[8], Hash{}, Hash{}}},
	Log{BlockNumber: 3, Address: addresses[8], Topics: [4]Hash{Hash{}, Hash{}, Hash{}, Hash{}}},
	Log{BlockNumber: 3, Address: addresses[6], Topics: [4]Hash{topics[5], topics[7], Hash{}, Hash{}}},

	Log{BlockNumber: 4, Address: addresses[1], Topics: [4]Hash{Hash{}, Hash{}, Hash{}, Hash{}}},
	Log{BlockNumber: 4, Address: addresses[9], Topics: [4]Hash{topics[8], topics[7], topics[3], Hash{}}},
	Log{BlockNumber: 4, Address: addresses[1], Topics: [4]Hash{topics[0], topics[3], topics[5], topics[4]}},
	Log{BlockNumber: 4, Address: addresses[2], Topics: [4]Hash{topics[2], topics[1], topics[0], topics[7]}},

	Log{BlockNumber: 5, Address: addresses[7], Topics: [4]Hash{topics[3], topics[5], Hash{}, Hash{}}},
	Log{BlockNumber: 5, Address: addresses[1], Topics: [4]Hash{topics[1], topics[2], topics[9], Hash{}}},
	Log{BlockNumber: 5, Address: addresses[1], Topics: [4]Hash{topics[0], topics[2], topics[3], topics[8]}},
	Log{BlockNumber: 5, Address: addresses[3], Topics: [4]Hash{topics[2], Hash{}, Hash{}, Hash{}}},
	Log{BlockNumber: 5, Address: addresses[6], Topics: [4]Hash{Hash{}, Hash{}, Hash{}, Hash{}}},
	Log{BlockNumber: 5, Address: addresses[0], Topics: [4]Hash{Hash{}, Hash{}, Hash{}, Hash{}}},
	Log{BlockNumber: 5, Address: addresses[4], Topics: [4]Hash{topics[4], topics[2], Hash{}, Hash{}}},

	Log{BlockNumber: 6, Address: addresses[4], Topics: [4]Hash{topics[9], topics[8], Hash{}, Hash{}}},
}

// ----------------------
// Section 2: Tests
// ----------------------

func TestNew(t *testing.T) {
	conn := makeDB(t)
	defer conn.Close()

	// get last block on empty DB
	count, err := conn.GetLastBlock()
	require.NoError(t, err, "GetLastBlock")
	require.Equal(t, count, uint64(0))
}

// TODO: TestUseAfterFree

func TestEmptyInsert(t *testing.T) {
	conn := makeDB(t)
	defer conn.Close()

	err := conn.Insert([]Log{})
	require.NoError(t, err, "GetLastBlock")

	count, err := conn.GetLastBlock()
	require.NoError(t, err, "GetLastBlock")
	require.Equal(t, count, uint64(0))
}

func TestQuery(t *testing.T) {
	conn := makeDB(t)
	defer conn.Close()

	err := conn.Insert(suite)
	require.NoError(t, err, "Insert")

	last, err := conn.GetLastBlock()
	require.NoError(t, err, "GetLastBLock")
	require.Equal(t, last, uint64(6))

	query := func(result uint64, q Query) {
		count, err := conn.Query(&q)
		require.NoError(t, err, "Query")
		require.Equal(t, count, result)
	}

	// Full scan
	query(20, Query{FromBlock: 0, ToBlock: 6, Addresses: []Address{}, Topics: [][]Hash{}})

	// Segment too large
	query(20, Query{FromBlock: 0, ToBlock: 42, Addresses: []Address{}, Topics: [][]Hash{}})

	// One block
	query(1, Query{FromBlock: 6, ToBlock: 6, Addresses: []Address{}, Topics: [][]Hash{}})
	query(7, Query{FromBlock: 5, ToBlock: 5, Addresses: []Address{}, Topics: [][]Hash{}})

	// Correct segment
	query(9, Query{FromBlock: 2, ToBlock: 4, Addresses: []Address{}, Topics: [][]Hash{}})

	// Address
	query(2, Query{
		FromBlock: 0, ToBlock: 6,
		Addresses: []Address{addresses[4]},
		Topics:    [][]Hash{},
	})

	// Some addresses
	query(3, Query{
		FromBlock: 0, ToBlock: 6,
		Addresses: []Address{addresses[3], addresses[4]},
		Topics:    [][]Hash{},
	})

	// Topics
	query(2, Query{
		FromBlock: 0, ToBlock: 6,
		Addresses: []Address{},
		Topics:    [][]Hash{[]Hash{}, []Hash{}, []Hash{topics[3]}},
	})
}
*/
