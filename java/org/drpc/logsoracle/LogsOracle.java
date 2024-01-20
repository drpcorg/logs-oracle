package org.drpc.logsoracle;

import static java.lang.foreign.MemorySegment.*;
import static java.lang.foreign.ValueLayout.*;

import java.io.*;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.net.URL;
import java.nio.file.*;
import java.util.List;

class Constants {
    static final int HASH_LENGTH = 32;
    static final int ADDRESS_LENGTH = 20;
    static final int TOPICS_LENGTH = 4;

    static final int RCLE_OK = 0;
    static final int RCLE_QUERY_OVERFLOW = 1;
    static final int RCLE_INVALID_DATADIR = 2;
    static final int RCLE_INVALID_UPSTREAM = 3;
    static final int RCLE_TOO_LARGE_QUERY = 4;
    static final int RCLE_NODE_REQUEST = 5;
    static final int RCLE_OUT_OF_MEMORY = 6;
    static final int RCLE_FILESYSTEM = 7;
    static final int RCLE_LIBCURL = 8;
    static final int RCLE_UNKNOWN = 9;

    static final OfBoolean C_BOOL_LAYOUT = JAVA_BOOLEAN;
    static final OfByte C_CHAR_LAYOUT = JAVA_BYTE;
    static final OfShort C_SHORT_LAYOUT = JAVA_SHORT;
    static final OfInt C_INT_LAYOUT = JAVA_INT;
    static final OfLong C_LONG_LAYOUT = JAVA_LONG;
    static final OfLong C_LONG_LONG_LAYOUT = JAVA_LONG;
    static final OfFloat C_FLOAT_LAYOUT = JAVA_FLOAT;
    static final OfDouble C_DOUBLE_LAYOUT = JAVA_DOUBLE;
    static final OfAddress C_POINTER_LAYOUT = ADDRESS.withBitAlignment(64).asUnbounded();
}

class rcl_query_address {
    static final StructLayout LAYOUT =
            MemoryLayout
                    .structLayout(Constants.C_POINTER_LAYOUT.withName("encoded"),
                            Constants.C_LONG_LONG_LAYOUT.withName("_hash"),
                            MemoryLayout.sequenceLayout(20, Constants.C_CHAR_LAYOUT)
                                    .withName("_data"),
                            MemoryLayout.paddingLayout(32))
                    .withName("rcl_query_address");

    static final VarHandle encoded_VH =
            LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("encoded"));
}

class rcl_query_topics {
    static final StructLayout LAYOUT =
            MemoryLayout
                    .structLayout(Constants.C_POINTER_LAYOUT.withName("encoded"),
                            Constants.C_LONG_LONG_LAYOUT.withName("_hash"),
                            MemoryLayout.sequenceLayout(32, Constants.C_CHAR_LAYOUT)
                                    .withName("_data"))
                    .withName("rcl_query_topics");

    static final VarHandle encoded_VH =
            LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("encoded"));
}

class rcl_query_t {
    static final StructLayout LAYOUT = MemoryLayout.structLayout(
            Constants.C_POINTER_LAYOUT.withName("address"),
            MemoryLayout.sequenceLayout(Constants.TOPICS_LENGTH, Constants.C_POINTER_LAYOUT)
                    .withName("topics"),
            Constants.C_LONG_LONG_LAYOUT.withName("from"),
            Constants.C_LONG_LONG_LAYOUT.withName("to"),
            Constants.C_LONG_LONG_LAYOUT.withName("limit"),
            Constants.C_LONG_LONG_LAYOUT.withName("alen"),
            MemoryLayout.sequenceLayout(Constants.TOPICS_LENGTH, Constants.C_LONG_LONG_LAYOUT)
                    .withName("tlen"),
            Constants.C_BOOL_LAYOUT, Constants.C_BOOL_LAYOUT, // _has_addresses, _has_topics
            MemoryLayout.paddingLayout(48));

    static final VarHandle from_VH =
            LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("from"));
    static final VarHandle to_VH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("to"));
    static final VarHandle limit_VH =
            LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("limit"));

    static final VarHandle alen_VH =
            LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("alen"));

    public static MemorySegment tlen_slice(MemorySegment seg) {
        return seg.asSlice(72, 32);
    }

    static final VarHandle address_VH =
            LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("address"));

    public static MemorySegment topics_slice(MemorySegment seg) {
        return seg.asSlice(8, 32);
    }
}

public class LogsOracle implements AutoCloseable {
    private Arena connArena = Arena.openShared();
    private MemorySegment connPtr;

    static {
        try {
            String lib_filename = "/liblogsoracle-";

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

            Path lib_file = Files.createTempFile("liblogsoracle", ".so");
            Files.copy(lib_packed.openStream(), lib_file, StandardCopyOption.REPLACE_EXISTING);
            new File(lib_file.toString()).deleteOnExit();

            System.load(lib_file.toAbsolutePath().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup SYMBOL_LOOKUP;

    static {
        SymbolLookup loaderLookup = SymbolLookup.loaderLookup();
        SYMBOL_LOOKUP = name -> loaderLookup.find(name).or(() -> LINKER.defaultLookup().find(name));
    }

    static MethodHandle downcallHandle(String name, FunctionDescriptor fdesc) {
        return SYMBOL_LOOKUP.find(name)
                .map(addr -> LINKER.downcallHandle(addr, fdesc))
                .orElse(null);
    }

    static final MethodHandle rcl_open_MH = downcallHandle("rcl_open",
            FunctionDescriptor.of(
                    Constants.C_INT_LAYOUT,
                    Constants.C_POINTER_LAYOUT,
                    Constants.C_LONG_LONG_LAYOUT,
                    Constants.C_POINTER_LAYOUT));
    static final MethodHandle rcl_free_MH = downcallHandle("rcl_free",
            FunctionDescriptor.ofVoid(
                    Constants.C_POINTER_LAYOUT));
    static final MethodHandle rcl_update_height_MH = downcallHandle("rcl_update_height",
            FunctionDescriptor.of(
                    Constants.C_INT_LAYOUT,
                    Constants.C_POINTER_LAYOUT,
                    Constants.C_LONG_LONG_LAYOUT));
    static final MethodHandle rcl_set_upstream_MH = downcallHandle("rcl_set_upstream",
            FunctionDescriptor.of(
                    Constants.C_INT_LAYOUT,
                    Constants.C_POINTER_LAYOUT,
                    Constants.C_POINTER_LAYOUT));
    static final MethodHandle rcl_query_MH = downcallHandle("rcl_query",
            FunctionDescriptor.of(
                    Constants.C_INT_LAYOUT,
                    Constants.C_POINTER_LAYOUT,
                    Constants.C_POINTER_LAYOUT,
                    Constants.C_POINTER_LAYOUT));
    static final MethodHandle rcl_logs_count_MH = downcallHandle("rcl_logs_count",
            FunctionDescriptor.of(
                    Constants.C_INT_LAYOUT,
                    Constants.C_POINTER_LAYOUT,
                    Constants.C_POINTER_LAYOUT));
    static final MethodHandle rcl_blocks_count_MH = downcallHandle("rcl_blocks_count",
            FunctionDescriptor.of(
                    Constants.C_INT_LAYOUT,
                    Constants.C_POINTER_LAYOUT,
                    Constants.C_POINTER_LAYOUT));
    static final MethodHandle rcl_strerror_MH = downcallHandle("rcl_strerror",
            FunctionDescriptor.of(
                    Constants.C_POINTER_LAYOUT,
                    Constants.C_INT_LAYOUT));

    public LogsOracle(String dir, long ram_limit) throws LogsOracleException {
        try (Arena arena = Arena.openConfined()) {
            var dbPtr = connArena.allocate(Constants.C_POINTER_LAYOUT);

            int rc;
            try {
                rc = (int) rcl_open_MH.invokeExact(connArena.allocateUtf8String(dir), ram_limit, dbPtr);
            } catch (Throwable ex) {
                throw new AssertionError("should not reach here", ex);
            }

            if (rc != Constants.RCLE_OK) {
                throw exception(rc);
            }

            connPtr = dbPtr.get(Constants.C_POINTER_LAYOUT, 0);
        }
    }

    public void UpdateHeight(long height) throws LogsOracleException {
        int rc;
        try {
            rc = (int) rcl_update_height_MH.invokeExact(connPtr, height);
        } catch (Throwable ex) {
            throw new AssertionError("should not reach here", ex);
        }

        if (rc != Constants.RCLE_OK)
            throw exception(rc);
    }

    public void SetUpstream(String upstream) throws LogsOracleException {
        try (Arena arena = Arena.openConfined()) {
            int rc;
            try {
                rc = (int) rcl_set_upstream_MH.invokeExact(connPtr, arena.allocateUtf8String(upstream));
            } catch (Throwable ex) {
                throw new AssertionError("should not reach here", ex);
            }

            if (rc != Constants.RCLE_OK)
                throw exception(rc);
        }
    }

    public long Query(
            Long limit,
            long fromBlock, long toBlock,
            List<String> address,
            List<List<String>> topics) throws LogsOracleException {
        try (Arena arena = Arena.openConfined()) {
            var query = arena.allocate(rcl_query_t.LAYOUT);

            rcl_query_t.from_VH.set(query, fromBlock);
            rcl_query_t.to_VH.set(query, toBlock);
            rcl_query_t.limit_VH.set(query, limit == null ? -1L : limit.longValue());

                    // address
                    long alen = address.size();
                    rcl_query_t.alen_VH.set(query, alen);
                    if (alen > 0) {
                        var addressPtr = arena.allocateArray(rcl_query_address.LAYOUT, alen);
                        for (int i = 0; i < alen; ++i) {
                            var item = addressPtr.asSlice(i * rcl_query_address.LAYOUT.byteSize());
                            rcl_query_address.encoded_VH.set(
                                    item, arena.allocateUtf8String(address.get(i)));
                        }
                        rcl_query_t.address_VH.set(query, addressPtr);
                    } else {
                        rcl_query_t.address_VH.set(query, NULL);
                    }

                    // topics
                    var tlenM = rcl_query_t.tlen_slice(query);
                    var topicsM = rcl_query_t.topics_slice(query);

                    for (int i = 0; i < Constants.TOPICS_LENGTH; ++i) {
                        var size = topics.size();
                        var topicM = topicsM.asSlice(i * Constants.C_POINTER_LAYOUT.byteSize());

                        if (size <= i) {
                            topicM.set(Constants.C_POINTER_LAYOUT, 0, NULL);
                            tlenM.set(ValueLayout.JAVA_LONG, i * ValueLayout.JAVA_LONG.byteSize(),
                                    0L);
                        } else {
                            var current = topics.get(i);
                            var topicPtr =
                                    arena.allocateArray(rcl_query_topics.LAYOUT, current.size());

                            for (int j = 0; j < current.size(); ++j) {
                                var item = topicPtr.asSlice(j * rcl_query_topics.LAYOUT.byteSize());
                                rcl_query_topics.encoded_VH.set(
                                        item, arena.allocateUtf8String(current.get(j)));
                            }

                            topicM.set(Constants.C_POINTER_LAYOUT, 0, topicPtr);
                            tlenM.set(ValueLayout.JAVA_LONG, i * ValueLayout.JAVA_LONG.byteSize(),
                                    topics.get(i).size());
                        }
                    }

                    // call
                    var result = arena.allocate(Constants.C_POINTER_LAYOUT);

                    int rc;
                    try {
                        rc = (int) rcl_query_MH.invokeExact(connPtr, query, result);
                    } catch (Throwable ex) {
                        throw new AssertionError("should not reach here", ex);
                    }
                    if (rc != Constants.RCLE_OK) throw exception(rc);

                    return result.get(ValueLayout.JAVA_LONG, 0);
            }
        }

        public long GetLogsCount() throws LogsOracleException {
            try (Arena arena = Arena.openConfined()) {
                var result = arena.allocate(Constants.C_POINTER_LAYOUT);

                int rc;
                try {
                    rc = (int) rcl_logs_count_MH.invokeExact(connPtr, result);
                } catch (Throwable ex) {
                    throw new AssertionError("should not reach here", ex);
                }
                if (rc != Constants.RCLE_OK) throw exception(rc);

                return result.get(ValueLayout.JAVA_LONG, 0);
            }
        }

        public long GetBlocksCount() throws LogsOracleException {
            try (Arena arena = Arena.openConfined()) {
                var result = arena.allocate(Constants.C_POINTER_LAYOUT);

                int rc;
                try {
                    rc = (int) rcl_blocks_count_MH.invokeExact(connPtr, result);
                } catch (Throwable ex) {
                    throw new AssertionError("should not reach here", ex);
                }

                if (rc != Constants.RCLE_OK) throw exception(rc);

                return result.get(ValueLayout.JAVA_LONG, 0);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                rcl_free_MH.invokeExact(connPtr);
            } catch (Throwable ex) {
                throw new AssertionError("should not reach here", ex);
            }
            connArena.close();
        }

        LogsOracleException exception(int code) {
            try {
                MemorySegment error = (MemorySegment) rcl_strerror_MH.invokeExact(code);
                return new LogsOracleException(code, error.getUtf8String(0));
            } catch (Throwable ex) {
                throw new AssertionError("should not reach here", ex);
            }
        }

        public class LogsOracleException extends Exception {
            private int code;

            public LogsOracleException(int code, String s) {
                super("liboracle error: " + s);
                this.code = code;
            }

            public boolean isQueryOverflow() {
                return code == Constants.RCLE_QUERY_OVERFLOW;
            }
        }
    }
