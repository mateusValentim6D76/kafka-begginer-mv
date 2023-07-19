package br.com.mv.learning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.logging.Logger;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName())
    public static void main(String[] args) {
        log.info("Hello Producer");

        //Create Producer properties
        Properties prop = new Properties();

        // Connect to localhost
//        prop.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Connect to Conduktor Playground
        prop.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        prop.setProperty("security.protocol", "SASL_SSL");
        prop.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"1GWUkwouULDhZPEKQlIaP3\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxR1dVa3dvdVVMRGhaUEVLUWxJYVAzIiwib3JnYW5pemF0aW9uSWQiOjc0NjYyLCJ1c2VySWQiOjg2ODc0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJjNTAxNzdiZS1lOTU5LTRlNmMtODIwYy0zMDQ5ZWE0MjYyMzYifX0.TrhtkmE2S2fmeTld1C_a--F-oWn9AE_UpJWXA9IX0FM\";");
        prop.setProperty("sasl.mechanism", "PLAIN");

        // Set Producer properties
        prop.setProperty("key.serializer", StringSerializer.class.getName());
        prop.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // Create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello my new Producer");

        // Send data
        producer.send(producerRecord);

        // Tell the Producer to send all data and block until done -- synchronous
        producer.flush();

        // Flush and close Producer
        producer.close();
    }
}
