package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class MyProducer {


    public static void main(String[] args) {
        uploadTestData();
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public static void deleteTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<String> topics = Arrays.asList("raw_data", "valid_data", "monitoring");

            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);

            deleteTopicsResult.all().get();

            System.out.println("Topics deleted successfully");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void uploadTestData() {
        try (KafkaProducer<String, String> producer = createKafkaProducer()) {
            String filePath = "/home/avtandil/IdeaProjects/FinalProjects/raw_data.csv";
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                int breakPoint = 0;
                while ((line = br.readLine()) != null && breakPoint <= 10) {
                    if (breakPoint == 0) {
                        breakPoint++;

                        continue;
                    }
                    producer.send(new ProducerRecord<>("raw_data", line));


                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                producer.close();
            }

        } catch (Exception e) {
            e.printStackTrace();

        }


    }
}
