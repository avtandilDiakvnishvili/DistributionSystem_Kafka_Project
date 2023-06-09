package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) throws AnalysisException, InterruptedException, TimeoutException, StreamingQueryException {


        MyProducer.uploadTestData();


        SparkSession spark = SparkSession.builder()
                .appName("Streaming Example")
                .master("local[4]")
                .getOrCreate();

        // Read data from a streaming source
        Dataset<Row> data = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "raw_data")
                .load();

        Dataset<Row> parsedData = data
                .selectExpr("CAST(value AS STRING)")
                .select(functions.split(functions.col("value"), ";").as("data"))
                .select(
                        functions.col("data").getItem(0).as("ts"),
                        functions.col("data").getItem(1).as("station_id"),
                        functions.col("data").getItem(2).cast("double").as("sensor0"),
                        functions.col("data").getItem(3).cast("double").as("sensor1"),
                        functions.col("data").getItem(4).cast("double").as("sensor2"),
                        functions.col("data").getItem(5).cast("double").as("sensor3")
                );


        String[] sensorColumns = {"sensor0", "sensor1", "sensor2", "sensor3"};
        for (String sensorColumn : sensorColumns) {
            parsedData = parsedData.withColumn(sensorColumn, when(col(sensorColumn).isNull(), Double.NaN).otherwise(col(sensorColumn)));
        }

        var filterData = parsedData.filter(col("ts").notEqual("1970-01-01 00:00:00"))
                .filter(col("sensor0").notEqual(Double.NaN).or(col("sensor1").notEqual(Double.NaN))
                        .or(col("sensor2").notEqual(Double.NaN)).or(col("sensor3").notEqual(Double.NaN))
                );

        filterData
                .selectExpr("CAST(ts AS STRING) as key", "CONCAT_WS(';', ts, station_id, sensor0,sensor1,sensor2,sensor3) AS value")
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "valid_data")
                .save();


    }
}