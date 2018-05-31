package com.landoop.jdbc.spark;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.jdbc.JdbcDialects;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaJdbcDemo {

    private static void createOutputTopic(String topic) throws IOException, RestClientException {

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", "PLAINTEXT://127.0.0.1:9092");

        AdminClient client = AdminClient.create(adminProps);
        client.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)));

        org.apache.avro.Schema schema = SchemaBuilder.record("agg_payment").fields()
                .name("currency").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("agg_amount").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
                .endRecord();

        SchemaRegistryClient registryClient = new CachedSchemaRegistryClient("http://localhost:8081", 1);
        registryClient.register(topic + "-key", SchemaBuilder.builder().stringType());
        registryClient.register(topic + "-value", schema);
    }

    public static void main(String[] args) throws IOException, RestClientException {

        JdbcDialects.registerDialect(new LsqlJdbcDialect());
        createOutputTopic("aggregated_payments");

        SparkConf conf = new SparkConf().setAppName("lenses-jdbc-spark").setMaster("local[4]");
        SparkContext context = new SparkContext(conf);
        SQLContext sql = new SQLContext(context);

        String uri = "jdbc:lsql:kafka:http://localhost:3030";
        Properties readProps = new Properties();
        readProps.setProperty("user", "admin");
        readProps.setProperty("password", "admin");

        Dataset<Row> df = sql.read().jdbc(uri, "SELECT currency, amount FROM cc_payments WHERE _vtype='AVRO' AND _ktype='BYTES' LIMIT 10000", readProps);
        Dataset<Row> aggregatedByCurrency = df.groupBy("currency").agg(org.apache.spark.sql.functions.sum("amount").alias("agg_amount"));
        List<Row> rows = aggregatedByCurrency.collectAsList();
        System.out.print(rows);

        Properties writeProps = new Properties();
        writeProps.setProperty("user", "admin");
        writeProps.setProperty("password", "admin");
        writeProps.setProperty("batchsize", "1000");
        aggregatedByCurrency.write().mode("append").jdbc(uri, "aggregated_payments", writeProps);
    }
}