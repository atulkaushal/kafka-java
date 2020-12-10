package com.github.atulkaushal.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class ProducerWithCallBackKeys.
 *
 * @author Atul
 */
public class ProducerWithCallBackKeys {

  /** The Constant BOOTSTRAP_SERVERS. */
  private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

  /**
   * The main method.
   *
   * @param args the arguments
   * @throws InterruptedException the interrupted exception
   * @throws ExecutionException the execution exception
   */
  public static void main(String... args) throws InterruptedException, ExecutionException {
    final Logger logger = LoggerFactory.getLogger(ProducerWithCallBackKeys.class);
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
      String topic = "first-topic";
      String value = "Hello-world-" + i;
      String key = "id_" + i;

      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

      logger.info("key: " + key);
      // id_0 partition 1
      // id_1 partition 0
      // id_2 partition 2
      // id_3 partition 0
      // id_4 partition 2
      // id_5 partition 2
      // id_6 partition 0
      // id_7 partition 2
      // id_8 partition 1
      // id_9 partition 2
      // id_10 partition 2

      // Send data.
      producer
          .send(
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
              })
          .get(); // block the .send(). Not for production.
      // flush data
      producer.flush();
    }
    // flush and close producer.
    producer.close();
  }
}
