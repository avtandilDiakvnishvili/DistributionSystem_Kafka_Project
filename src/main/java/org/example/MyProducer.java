package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.example.SparkKafka.server;

public class MyProducer {
    public static void main(String[] args) {
        uploadTestData();
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        return producer;
    }

    private static void deleteTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<String> topics = Arrays.asList("raw_data", "valid_data");

            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);

            deleteTopicsResult.all().get();

            System.out.println("Topics deleted successfully");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void uploadTestData() {
        KafkaProducer<String, String> producer = createKafkaProducer();
        try {
            String filePath = "/home/avtandil/IdeaProjects/FinalProjects/raw_data.csv";
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                int breakPoint = 0;
                while ((line = br.readLine()) != null && breakPoint <= 10) {
                    producer.send(new ProducerRecord<>("raw_data", line));
                    breakPoint++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                producer.close();
            }

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }


    }
}
