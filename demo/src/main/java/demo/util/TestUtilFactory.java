package demo.util;

public class TestUtilFactory {
    public static TestUtil create(TestDbms testDbms) {
        switch (testDbms) {
            case POSTGRES:
                return new PostgresTestUtil();
            case DUCKDB:
                return new DuckDbTestUtil();
            case MYSQL:
                return new MySqlTestUtil();
            default:
                throw new IllegalArgumentException("Unsupported TestDbms: " + testDbms);
        }
    }
}
