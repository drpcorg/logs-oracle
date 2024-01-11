import org.drpc.logsoracle.*;

import java.io.File;
import java.util.Collections;

class Example {
    public static void main(String[] args) throws Exception {
        try (LogsOracle db = new LogsOracle("_data", 0L)) {
            db.UpdateHeight(42L);

            System.out.println("Logs: " + db.GetLogsCount());
            System.out.println("Blocks: " + db.GetBlocksCount());
            System.out.println("Query: "
                    + db.Query(null, 1L, 16L, Collections.emptyList(), Collections.emptyList()));
        }
    }
}
