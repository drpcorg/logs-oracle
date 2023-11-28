package org.drpc.logsoracle;

import static java.lang.foreign.MemorySegment.NULL;

import java.io.*;
import java.lang.UnsupportedOperationException;
import java.lang.foreign.*;
import java.net.URL;
import java.nio.file.*;
import java.util.List;

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

            System.out.println(lib_filename);

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
        var dbPtr = arena.allocate(liboracle_h.C_POINTER);
        int rc = liboracle_h.rcl_open(arena.allocateUtf8String(dir), 0L, dbPtr);
        if (rc != liboracle_h.RCL_SUCCESS()) {
            throw new Exception("liboracle connection failed");
        } else {
            connPtr = dbPtr.get(liboracle_h.C_POINTER, 0);
        }
    }

    public void UpdateHeight(Long height) throws Exception {
        int rc = liboracle_h.rcl_update_height(connPtr, height);
        if (rc != liboracle_h.RCL_SUCCESS()) {
            throw new Exception("liboracle failed");
        }
    }

    public void SetUpstream(String upstream) throws Exception {
        int rc = liboracle_h.rcl_set_upstream(connPtr, arena.allocateUtf8String(upstream));
        if (rc != liboracle_h.RCL_SUCCESS()) {
            throw new Exception("liboracle connection failed");
        }
    }

    public long Query(
        Long fromBlock,
        Long toBlock,
        List<String> address,
        List<List<String>> topics
    ) throws Exception {
        // TODO: liboracle_h.rcl_query(connPtr);
        throw new UnsupportedOperationException();
    }

    public void Insert() throws Exception {
        // TODO: liboracle_h.rcl_insert(connPtr);
        throw new UnsupportedOperationException();
    }

    public long GetLogsCount() throws Exception {
        var result = arena.allocate(liboracle_h.C_POINTER);

        int rc = liboracle_h.rcl_logs_count(connPtr, result);
        if (rc != liboracle_h.RCL_SUCCESS())
            throw new Exception("liboracle failed");

        return result.get(ValueLayout.JAVA_LONG, 0);
    }

    public long GetBlocksCount() throws Exception {
        var result = arena.allocate(liboracle_h.C_POINTER);

        int rc = liboracle_h.rcl_blocks_count(connPtr, result);
        if (rc != liboracle_h.RCL_SUCCESS())
            throw new Exception("liboracle failed");

        return result.get(ValueLayout.JAVA_LONG, 0);
    }

    @Override
    public void close() throws IOException {
        liboracle_h.rcl_free(connPtr);
        arena.close();
    }
}
