package demo;

import demo.util.TestDbms;

public enum DataType {
    INT, BOOLEAN, FLOAT, DOUBLE, TEXT, VARCHAR, DATE, DATETIME, TIME;

    /**
     * Handle nuances in data types naming between databases.
     */
    public String toString() throws IllegalArgumentException {
        switch (this) {
            case INT:
                return "INT";
            case BOOLEAN:
                return "BOOLEAN";
            case FLOAT:
                if (GlobalInfo.testDbms == TestDbms.POSTGRES ||
                        GlobalInfo.testDbms == TestDbms.DUCKDB) {
                    return "REAL";
                }
                return "FLOAT";
            case DOUBLE:
                if (GlobalInfo.testDbms == TestDbms.POSTGRES) {
                    return "DOUBLE PRECISION";
                }
                if (GlobalInfo.testDbms == TestDbms.MYSQL) {
                    return "DOUBLE(10,2)";
                }
                return "DOUBLE";
            case TEXT:
                return "TEXT";
            case DATE:
                return "DATE";
            case DATETIME:
                if (GlobalInfo.testDbms == TestDbms.POSTGRES ||
                        GlobalInfo.testDbms == TestDbms.DUCKDB) {
                    return "TIMESTAMP";
                }
                return "DATETIME";
            case TIME:
                return "TIME";
            case VARCHAR:
                if (GlobalInfo.testDbms == TestDbms.MYSQL) {
                    return "VARCHAR(255)";
                }
                return "VARCHAR";
            default:
                throw new IllegalArgumentException("Unknown type: " + this);
        }
    }

    public static DataType getRandomDataType() {
        return Randomly.fromOptions(DataType.values());
    }
}
