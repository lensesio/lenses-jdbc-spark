package com.landoop.jdbc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.jdbc.JdbcDialects;

import java.util.List;
import java.util.Properties;

public class KafkaJdbcDemo {

    public static void main(String[] args) {

        JdbcDialects.registerDialect(new LsqlJdbcDialect());

        SparkConf conf = new SparkConf().setAppName("lenses-jdbc-spark").setMaster("local[4]");
        SparkContext context = new SparkContext(conf);
        SQLContext sql = new SQLContext(context);

        String uri = "jdbc:lsql:kafka:https://hostname:port";
        Properties props = new Properties();
        props.setProperty("user", "username");
        props.setProperty("password", "password");

        Dataset<Row> df = sql.read().jdbc(uri, "SELECT currency, amount FROM `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING' LIMIT 1000000", props);
        Dataset<Row> aggregatedByCurrency = df.groupBy("currency").sum("amount");
        List<Row> rows = aggregatedByCurrency.collectAsList();
        System.out.print(rows);
    }
}
