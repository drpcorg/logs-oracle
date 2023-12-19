import java.io.File;
import java.util.Collections;
import org.drpc.logsoracle.*;

class Example {
    public static void main(String[] args) throws Exception {
        File dir = new File("./_data");
        if (!dir.exists()){
            dir.mkdirs();
        }

        try (LogsOracle db = new LogsOracle(dir.getCanonicalPath(), 0L)) {
            System.out.println(db.GetLogsCount());
            System.out.println(db.GetBlocksCount());
            System.out.println(db.Query(0L, 16L, Collections.emptyList(), Collections.emptyList()));
        }
    }
}
