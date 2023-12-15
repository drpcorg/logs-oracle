package org.drpc.logsoracle;

import static java.lang.foreign.MemorySegment.NULL;

import java.io.*;
import java.lang.UnsupportedOperationException;
import java.lang.foreign.*;
import java.net.URL;
import java.nio.file.*;
import java.util.List;

import static org.drpc.logsoracle.liboracle_h.*;

public class LogsOracle implements AutoCloseable {
    private Arena arena = Arena.openShared();
    private MemorySegment connPtr;

    static {
        try {
            String lib_filename = "/liboracle-";

            lib_filename += switch (System.getProperty("os.arch").toLowerCase().trim()) {
                case "x86_64", "amd64" -> "x86_64";
                case "aarch64", "arm64" -> "arm64";
                case "i386" -> "i386";
                default -> throw new IllegalStateException("Unsupported system architecture");
            };

            String os_name_detect = System.getProperty("os.name").toLowerCase().trim();
            if (os_name_detect.startsWith("windows")) {
                lib_filename += "-windows.dll";

                // TODO: windows support
                throw new IllegalStateException("Unsupported windows");
            } else if (os_name_detect.startsWith("mac")) {
                lib_filename += "-macos.dylib";
            } else if (os_name_detect.startsWith("linux")) {
                lib_filename += "-linux.so";
            }

            URL lib_packed = LogsOracle.class.getResource(lib_filename);
            if (lib_packed == null) {
                throw new IOException(lib_packed + " not found");
            }

            Path lib_file = Files.createTempFile("liboracle_java_native", ".dynlib");
            Files.copy(lib_packed.openStream(), lib_file, StandardCopyOption.REPLACE_EXISTING);
            new File(lib_file.toString()).deleteOnExit();

            System.load(lib_file.toAbsolutePath().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public LogsOracle(String dir, long ram_limit) throws Exception {
        var dbPtr = arena.allocate(C_POINTER);
        int rc = rcl_open(arena.allocateUtf8String(dir), 0L, dbPtr);
        if (rc != RCL_SUCCESS()) {
            throw new Exception("liboracle connection failed");
        } else {
            connPtr = dbPtr.get(C_POINTER, 0);
        }
    }

    public void UpdateHeight(Long height) throws Exception {
        int rc = rcl_update_height(connPtr, height);
        if (rc != RCL_SUCCESS()) {
            throw new Exception("liboracle failed");
        }
    }

    public void SetUpstream(String upstream) throws Exception {
        int rc = rcl_set_upstream(connPtr, arena.allocateUtf8String(upstream));
        if (rc != RCL_SUCCESS()) {
            throw new Exception("liboracle connection failed");
        }
    }

    public long Query(
        Long fromBlock,
        Long toBlock,
        List<String> address,
        List<List<String>> topics
    ) throws Exception {
        var query = rcl_query_t.allocate(arena);

        rcl_query_t.from_block$set(query, fromBlock);
        rcl_query_t.to_block$set(query, toBlock);

        // address
        var c_address = rcl_query_t.address$slice(query);
        rcl_query_address.len$set(c_address, address.size());

        // topics
        var c_topics = rcl_query_t.topics$slice(query);

        // call
        var result = arena.allocate(C_POINTER);
        int rc = rcl_query(connPtr, query, result);
        if (rc != RCL_SUCCESS())
            throw new Exception("liboracle: failed query");

        return result.get(ValueLayout.JAVA_LONG, 0);
    }

    public void Insert() throws Exception {
        // TODO: rcl_insert(connPtr);
        throw new UnsupportedOperationException();
    }

    public long GetLogsCount() throws Exception {
        var result = arena.allocate(C_POINTER);

        int rc = rcl_logs_count(connPtr, result);
        if (rc != RCL_SUCCESS())
            throw new Exception("liboracle failed");

        return result.get(ValueLayout.JAVA_LONG, 0);
    }

    public long GetBlocksCount() throws Exception {
        var result = arena.allocate(C_POINTER);

        int rc = rcl_blocks_count(connPtr, result);
        if (rc != RCL_SUCCESS())
            throw new Exception("liboracle failed");

        return result.get(ValueLayout.JAVA_LONG, 0);
    }

    @Override
    public void close() throws IOException {
        rcl_free(connPtr);
        arena.close();
    }
}
