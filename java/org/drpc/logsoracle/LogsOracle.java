package org.drpc.logsoracle;

import static java.lang.foreign.MemorySegment.NULL;

import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.URL;
import java.nio.file.*;

public class LogsOracle implements AutoCloseable {
    private Arena arena;
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
        arena = Arena.openShared();

        connPtr = liboracle_h.rcl_new(arena.allocateUtf8String(dir), 0L);
        if (connPtr.equals(NULL)) {
            throw new Exception("liboracle connection failed");
        }
    }

    public long query() throws Exception {
        // liboracle_h.rcl_query(connPtr);
        return 0L;
    }

    public void insert() throws Exception {
        // liboracle_h.rcl_insert(connPtr);
    }

    public long GetLogsCount() {
        return liboracle_h.rcl_logs_count(connPtr);
    }

    public long GetBlocksCount() {
        return liboracle_h.rcl_blocks_count(connPtr);
    }

    @Override
    public void close() throws IOException {
        liboracle_h.rcl_free(connPtr);
        arena.close();
    }
}
