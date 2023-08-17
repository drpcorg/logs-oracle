CREATE TABLE IF NOT EXISTS Logs
(
	BlockNumber      UInt64,
	-- BlockHash        FixedString(66),

	-- TransactionIndex String,
	-- TransactionHash  String,

	-- LogIndex         String,
	Address          LowCardinality(FixedString(20)),
	Topics           Array(FixedString(32)),
	-- Data             String,
	-- Removed          Boolean
)
ENGINE = MergeTree
PRIMARY KEY (BlockNumber, Address, Topics);
