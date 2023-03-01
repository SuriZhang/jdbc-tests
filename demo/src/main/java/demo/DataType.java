package demo;

import demo.util.DuckDbTestUtil;
import demo.util.PostgresTestUtil;
import demo.util.TestUtil;

public class DataType {
    private static TestUtil testUtil;

    public DataType(TestUtil testUtil) {
        DataType.testUtil = testUtil;
    }

    public enum Type {
        INT, BOOLEAN, FLOAT, DOUBLE, TEXT, VARCHAR, DATE, DATETIME, TIME;

        /**
         *  Handle nuances in data types naming between databases.
         */
        public String toString() throws IllegalArgumentException {
            switch (this) {
                case INT:
                    return "INT";
                case BOOLEAN:
                    return "BOOLEAN";
                case FLOAT:
                    if (testUtil instanceof PostgresTestUtil || 
                        testUtil instanceof DuckDbTestUtil) {
                        return "REAL";
                    }
                    return "FLOAT";
                case DOUBLE:
                    if (testUtil instanceof PostgresTestUtil) {
                        return "DOUBLE PRECISION";
                    }
                    return "DOUBLE";
                case TEXT:
                    return "TEXT";
                case DATE:
                    return "DATE";
                case DATETIME:
                    if (testUtil instanceof PostgresTestUtil ||
                        testUtil instanceof DuckDbTestUtil) {
                        return "TIMESTAMP";
                    }
                    return "DATETIME";
                case TIME:
                    return "TIME";
                case VARCHAR:
                    return "VARCHAR";
                default:
                    throw new IllegalArgumentException("Unknown type: " + this);
            }
        }
    }

    
}
