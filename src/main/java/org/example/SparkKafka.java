package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.spark.sql.functions.*;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.

public class SparkKafka {
    static String server = "localhost:9092";
    private static final String HOST = "172.17.0.2"; // Replace with your Cassandra contact point
    private static final int PORT = 9042;
    private static final String LOCAL_DC = "datacenter1";

    public static void main(String[] args) {

        InetSocketAddress contactPoint = new InetSocketAddress(HOST, PORT);

        deleteTopics();
        uploadTestData();

        SparkSession spark = SparkSession.builder()
                .appName("Validate Data")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> streamingData = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "raw_data")
                .load();

        streamingData.show();

        Dataset<Row> parsedData = streamingData
                .selectExpr("CAST(value AS STRING)")
                .select(functions.split(functions.col("value"), ",").as("data"))
                .select(
                        functions.col("data").getItem(0).as("ts"),
                        functions.col("data").getItem(1).as("station_id"),
                        functions.col("data").getItem(2).cast("double").as("sensor0"),
                        functions.col("data").getItem(3).cast("double").as("sensor1"),
                        functions.col("data").getItem(4).cast("double").as("sensor2"),
                        functions.col("data").getItem(5).cast("double").as("sensor3")
                );

        Row firstRow = parsedData.first();

        Dataset<Row> filteredData = parsedData
                .filter(functions.not(functions.expr("ts = '" + firstRow.getString(0) + "' AND station_id = '" + firstRow.getString(1) + "'")));

        filteredData.show();

        Column average = functions.round(col("sensor0")
                .plus(col("sensor1"))
                .plus(col("sensor2"))
                .plus(col("sensor3"))
                .divide(4.0), 4);

        Column previousAverage = coalesce(lag(average, 1).over(Window.orderBy("ts")), lit(0));

        // Task 1: Ignore sensors outside [-100, 100] range or NaN
        Dataset<Row> filteredRange = parsedData.filter(
                functions.col("sensor0").between(-100, 100)
                        .and(functions.col("sensor1").between(-100, 100))
                        .and(functions.col("sensor2").between(-100, 100))
                        .and(functions.col("sensor3").between(-100, 100))
                        .and(functions.col("sensor0").isNotNull())
                        .and(functions.col("sensor1").isNotNull())
                        .and(functions.col("sensor2").isNotNull())
                        .and(functions.col("sensor3").isNotNull())
        );

        filteredRange.show();

        // Task 2: Ignore sensors that deviate by 2 degrees from the average of 4 sensors
        Dataset<Row> filteredAverageRange = filteredRange.filter(
                abs(functions.col("sensor0").minus(average))
                        .leq(lit(2.0))
                        .and(abs(functions.col("sensor1").minus(average)).leq(lit(2.0)))
                        .and(abs(functions.col("sensor2").minus(average)).leq(lit(2.0)))
                        .and(abs(functions.col("sensor3").minus(average)).leq(lit(2.0)))
        );

        filteredAverageRange.show();

        Dataset<Row> cleanedData = filteredAverageRange.select(col("ts"), col("station_id"), col("sensor0"), col("sensor1"), col("sensor2"), col("sensor3"))
                .withColumn("temperature", average)
                .withColumn("previous_average", previousAverage)
                .filter(col("temperature").notEqual(0.0).or(abs(col("previous_average")).leq(2.0)));

        cleanedData.show();

        cleanedData
                .selectExpr("CAST(ts AS STRING) AS key", "CONCAT_WS(';', ts, station_id, temperature) AS value")
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "valid_data")
                .save();
//        try (CqlSession session = CqlSession.builder()
//                .addContactPoint(contactPoint)
//                .withLocalDatacenter(LOCAL_DC)
//                .build()) {
//
//            System.out.println("Connected to Cassandra cluster!");
//
//            // Insert mock data into testdata table
//            String insertQuery = "INSERT INTO mykeyspace.testdata (ts, station_id, sensor0, sensor1, sensor2, sensor3, status) VALUES (?, ?, ?, ?, ?, ?, ?)";
//
//            // Prepare the insert statement
//            PreparedStatement insertStatement = session.prepare(insertQuery);
//            Dataset<Row> joinedData = filteredData
//                    .join(cleanedData, filteredData.col("ts").equalTo(cleanedData.col("ts"))
//                            .and(filteredData.col("station_id").equalTo(cleanedData.col("station_id"))), "left");
//
//            Dataset<Row> result = joinedData.withColumn("status",
//                            functions.when(joinedData.col("temperature").isNotNull(), "valid").otherwise("invalid"))
//                    .select(filteredData.col("ts"), filteredData.col("station_id"), filteredData.col("sensor0"), filteredData.col("sensor1"), filteredData.col("sensor2"), filteredData.col("sensor3"), col("status"));
//
//            result.show();
//
//            List<Row> rows = result.collectAsList();
//            for (Row row : rows) {
//                // Extract the values from the row
//                String ts = row.getAs("ts");
//                String stationId = row.getAs("station_id");
//                Double sensor0 = row.getAs("sensor0");
//                Double sensor1 = row.getAs("sensor1");
//                Double sensor2 = row.getAs("sensor2");
//                Double sensor3 = row.getAs("sensor3");
//                String status = row.getAs("status");
//
//                // Bind and execute the insert statement for each row
//                BoundStatement boundStatement = insertStatement.bind(ts, stationId, sensor0, sensor1, sensor2, sensor3, status);
//                session.execute(boundStatement);
//            }
//            System.out.println("Mock data inserted successfully.");
//
//        } catch (Exception e) {
//            System.out.println("An error occurred: " + e.getMessage());
//        }
    }

    private static void deleteTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<String> topics = Arrays.asList("valid_data", "clean");

            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);

            deleteTopicsResult.all().get();

            System.out.println("Topics deleted successfully");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void uploadTestData() {
        String filePath = "/home/avtandil/IdeaProjects/FinalProjects/raw_data.csv";
        KafkaProducer<String, String> producer = createKafkaProducer();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                producer.send(new ProducerRecord<>("raw_data", line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        return producer;
    }
}