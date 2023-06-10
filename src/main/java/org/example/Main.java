package org.example;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;


public class Main {
    public static void main(String[] args) {

        MyProducer.deleteTopics();
        MyProducer.uploadTestData();


        SparkSession spark = SparkSession.builder().appName("Streaming Example").master("local[*]").getOrCreate();

        // Read data from a streaming source
        Dataset<Row> data = spark.read().format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "raw_data").option("group.id", "test")
                .load();

        Dataset<Row> parsedData = data
                .selectExpr("CAST(value AS STRING)")
                .select(functions.split(functions.col("value"), ";")
                        .as("data"))
                .select(functions.col("data").getItem(0).as("ts"),
                        functions.col("data").getItem(1).as("station_id"),
                        functions.col("data").getItem(2).cast("double").as("sensor0"),
                        functions.col("data").getItem(3).cast("double").as("sensor1"),
                        functions.col("data").getItem(4).cast("double").as("sensor2"),
                        functions.col("data").getItem(5).cast("double").as("sensor3"));

        parsedData.cache();

        Dataset<Row> filteredParseData = null;
        String[] sensorColumns = {"sensor0", "sensor1", "sensor2", "sensor3"};
        for (String sensorColumn : sensorColumns) {
            filteredParseData = parsedData.withColumn(sensorColumn, when(col(sensorColumn).isNull(), Double.NaN).otherwise(col(sensorColumn)));
        }

        //validate data
        var filterData = filteredParseData.
                filter(col("ts")
                        .notEqual("1970-01-01 00:00:00"))
                .filter(col("sensor0").
                        notEqual(Double.NaN)
                        .or(col("sensor1").notEqual(Double.NaN))
                        .or(col("sensor2")
                                .notEqual(Double.NaN)).or(col("sensor3").notEqual(Double.NaN)));

        filterData.selectExpr("CAST(ts AS STRING) as key", "CONCAT_WS(';', ts, station_id, sensor0,sensor1,sensor2,sensor3) AS value").
                write().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "valid_data").save();


        //monitoring data


        Dataset<String> invalidDataMessages = parsedData.filter(col("ts").equalTo("1970-01-01 00:00:00")
                        .or(col("sensor0").isNull()
                                .and(col("sensor1").isNull())
                                .and(col("sensor2").isNull())
                                .and(col("sensor3").isNull())))
                .flatMap(new InvalidDataChecker(), Encoders.STRING());
        invalidDataMessages.show();
        invalidDataMessages
                .selectExpr("CAST(value AS STRING) as value")
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "monitoring").save();


    }
}