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
        var ctlen = arena.allocateArray(ValueLayout.JAVA_LONG,
            topics.size() > 0 ? topics.get(0).size() : 0,
            topics.size() > 1 ? topics.get(1).size() : 0,
            topics.size() > 2 ? topics.get(2).size() : 0,
            topics.size() > 3 ? topics.get(3).size() : 0
        );

        var queryPtrPtr = arena.allocate(C_POINTER); // rcl_query_t**
        int rc = rcl_query_alloc(queryPtrPtr, address.size(), ctlen);
        if (rc != RCL_SUCCESS())
            throw new Exception("liboracle: failed create query");

        var queryPtr = queryPtrPtr.get(C_POINTER, 0); // rcl_query_t*

        rcl_query_t.from$set(queryPtr, fromBlock);
        rcl_query_t.to$set(queryPtr, toBlock);

        // address
        var caddress = arena.allocateArray(rcl_query_address.$LAYOUT(), address.size());
        for (long i = 0; i < address.size(); ++i) {
            // var item = caddress.getAtIndex(rcl_query_address.$LAYOUT(), i);
        }

        rcl_query_t.alen$set(queryPtr, address.size());
        rcl_query_t.address$set(queryPtr, caddress);

        // topics
        // var ctlen = rcl_query_t.topics$slice(queryPtr);

        // call
        var result = arena.allocate(C_POINTER);
        rc = rcl_query(connPtr, queryPtr, result);
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
