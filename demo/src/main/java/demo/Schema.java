package demo;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Schema {
    public static class Table {
        final String tableName;
        List<Column> columns;

        public Table(String tableName, List<Column> columns) {
            this.tableName = tableName;
            this.columns = columns;

        }

        public void addColumn(Column column) {
            if (this.columns == null) {
                this.columns = new ArrayList<>();
            }
            this.columns.add(column);
        }

        public String columnsToString() {
            StringBuilder sb = new StringBuilder();
            for (Column column : this.columns) {
                sb.append(column.toString());
                sb.append(", ");
            }
            return sb.toString();
        }

        public String getName() {
            return this.tableName;
        }

        public List<Column> getColumns() {
            return this.columns;
        }
    }

    public static class Column {
        final String columnName;
        public DataType type;

        public Column(String columnName, DataType type) {
            this.columnName = columnName;
            this.type = type;
        }

        public String toString() {
            return this.columnName + " " + this.type.toString();
        }

        public String getName() {
            return this.columnName;
        }

        public DataType getType() {
            return this.type;
        }

    }

    public static String generateTableName(Connection connection) {
        List<String> tableNames = GlobalInfo.getTables(connection).stream().map(t -> t.getName())
                .collect(Collectors.toList());
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String tableName = String.format("table_%d", i++);
            if (tableNames.stream().noneMatch(t -> t.equalsIgnoreCase(tableName))) {
                return tableName;
            }
        } while (true);

    }

    public static String generateColumnName(Connection connection, String tableName) {
        List<String> columnNames = GlobalInfo.getTables(connection).stream()
                .filter(t -> t.getName().equalsIgnoreCase(tableName)).flatMap(t -> t.getColumns().stream())
                .map(c -> c.getName()).collect(Collectors.toList());
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String columnName = String.format("column_%d", i++);
            if (columnNames.stream().noneMatch(c -> c.equalsIgnoreCase(columnName))) {
                return columnName;
            }
        } while (true);
    }

    public static Column generateColumn(Connection connection, String tableName) {
        String columnName = generateColumnName(connection, tableName);
        DataType type = DataType.getRandomDataType();
        return new Column(columnName, type);
    }

    public static Table generateTable(Connection connection) {
        String tableName = generateTableName(connection);
        Table table = new Table(tableName, new ArrayList<>());
        int numColumns = Randomly.smallNumber() + 1;
        for (int i = 0; i < numColumns; i++) {
            table.addColumn(generateColumn(connection, tableName));
        }
        return table;
    }

    public static String generateSchemaName(Connection connection) {
        List<String> schemaNames = GlobalInfo.getSchemas(connection);
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String schemaName = String.format("schema_%d", i++);
            if (schemaNames.stream().noneMatch(s -> s.equalsIgnoreCase(schemaName))) {
                return schemaName;
            }
        } while (true);
    }

}
