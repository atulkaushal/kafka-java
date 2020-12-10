package com.github.atulkaushal.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * The Class Producer.
 *
 * @author Atul
 */
public class Producer {

  /** The Constant BOOTSTRAP_SERVERS. */
  private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String... args) {
    // Create Producer Properties

    Properties properties = new Properties();

    /*
     * You can either pass the property as hard-coded or use ProducerConfig class.
     *
     * properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
     * properties.setProperty("key.serializer", StringSerializer.class.getName());
     * properties.setProperty("value.serialzer", StringSerializer.class.getName());
     */

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create the Producer

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    // create a producer record.

    ProducerRecord<String, String> record =
        new ProducerRecord<String, String>("first-topic", "Hello-world");
    // Send data.
    producer.send(record);
    // flush data
    producer.flush();

    // flush and close producer.
    producer.close();
  }
}
