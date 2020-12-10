package com.github.atulkaushal.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class ProducerWithCallBack.
 *
 * @author Atul
 */
public class ProducerWithCallBack {

  /** The Constant BOOTSTRAP_SERVERS. */
  private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String... args) {
    final Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);
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

    for (int i = 0; i < 11; i++) {
      ProducerRecord<String, String> record =
          new ProducerRecord<String, String>("first-topic", "Hello-world-" + i);
      // Send data.
      producer.send(
          record,
          new Callback() {

            public void onCompletion(RecordMetadata metadata, Exception exception) {
              // executes every time a record is successfully sent or an exceptionis thrown.
              if (exception == null) {
                logger.info(
                    "received new metadata.\n"
                        + "Topic: "
                        + metadata.topic()
                        + "\n"
                        + "Partition: "
                        + metadata.partition()
                        + "\n"
                        + "offset: "
                        + metadata.offset()
                        + "\n"
                        + "TimeStamp: "
                        + metadata.timestamp());

              } else {
                logger.error(exception.getMessage());
              }
            }
          });
      // flush data
      producer.flush();
    }
    // flush and close producer.
    producer.close();
  }
}
