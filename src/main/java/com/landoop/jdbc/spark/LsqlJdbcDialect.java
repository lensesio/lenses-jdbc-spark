package com.landoop.jdbc.spark;

import com.landoop.jdbc4.LsqlDriver;
import org.apache.spark.sql.jdbc.JdbcDialect;

public class LsqlJdbcDialect extends JdbcDialect {

    static {
        new LsqlDriver();
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:lsql:kafka");
    }

    @Override
    public String getSchemaQuery(String table) {
        return table.replace("\\sLIMIT\\s+(\\d+)", "LIMIT 1");
    }

    @Override
    public String getTableExistsQuery(String table) {
        return table;
    }

    @Override
    public String quoteIdentifier(String colName) {
        return "`" + colName + "`";
    }

    @Override
    public String escapeSql(String value) {
        return value;
    }
}
