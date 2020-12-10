package com.github.atulkaushal.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class ConsumerAssignSeek.
 *
 * <p>Assign and seek approach is used rarely to read messages but it will be useful in case you
 * need to read a specific set of messages.
 *
 * @author Atul
 */
public class ConsumerAssignSeek {

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(ConsumerAssignSeek.class.getName());

    final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    String topic = "first-topic";

    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create the consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    // assign and seek are mostly used to replay data or fetch a specific message.

    // assign
    TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
    long offsetToReadFrom = 15l;
    consumer.assign(Arrays.asList(partitionToReadFrom));

    // seek
    consumer.seek(partitionToReadFrom, offsetToReadFrom);

    int numOfMessagesToRead = 5;
    boolean keepOnReading = true;
    int numOfmessagesReadSoFar = 0;

    // poll for new data
    while (keepOnReading) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        numOfmessagesReadSoFar++;
        logger.info("Key: " + record.key() + ", Value: " + record.value());
        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
        if (numOfmessagesReadSoFar >= numOfMessagesToRead) {
          keepOnReading = false;
          break;
        }
      }
    }

    logger.info("Exiting the application.");
    consumer.close();
  }
}
