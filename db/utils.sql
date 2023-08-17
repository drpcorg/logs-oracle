CREATE VIEW metadata IF NOT EXISTS AS
SELECT
	formatReadableQuantity(max(BlockNumber)) as blocks,
	formatReadableQuantity(count(*)) as logs,
	formatReadableQuantity(uniq(Address)) as uniq_addr,
	(SELECT BlockNumber, count(*) as logs FROM Logs GROUP BY BlockNumber ORDER BY logs DESC LIMIT 1) as biggest_block
FROM Logs;

CREATE VIEW size IF NOT EXISTS AS
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes) AS size) AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes) AS usize) AS uncompressed,
    round(usize / size, 2) AS compr_rate,
    sum(rows) AS rows,
    count() AS part_count
FROM system.parts
WHERE (active = 1) AND (database LIKE '%') AND (table LIKE '%')
GROUP BY
    database,
    table
ORDER BY size DESC;
