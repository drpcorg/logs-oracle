import org.drpc.logsoracle.*;

class Example {
    public static void main(String[] args) throws Exception {
        try (LogsOracle db = new LogsOracle("./_data", 0L)) {
            System.out.println(db.GetBlocksCount());
        }
    }
}
