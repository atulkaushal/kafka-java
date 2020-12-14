package com.github.atulkaushal.kafka.basics;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class ConsumerWithThread.
 *
 * @author Atul
 */
public class ConsumerWithThread {

  /** The logger. */
  Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class.getName());

  /** Instantiates a new consumer with thread. */
  ConsumerWithThread() {}

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {
    new ConsumerWithThread().run();
  }

  /** Run. */
  public void run() {
    final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    String groupId = "my-sixth-application";
    String topic = "first-topic";

    // latch for dealing with multiple threads.
    CountDownLatch latch = new CountDownLatch(1);

    // Create a consumer Runnable.
    Runnable myConsumerRunnable = new ConsumerRunnable(latch, BOOTSTRAP_SERVERS, groupId, topic);

    // start the thread
    Thread myThread = new Thread(myConsumerRunnable);
    myThread.start();

    // add a shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Caught shutdown hook");
                  ((ConsumerRunnable) myConsumerRunnable).shutdown();

                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                  }
                  logger.info("Application has exited");
                }));

    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Application got interupted", e);
    } finally {
      logger.info("Application is closing");
    }
  }

  /** The Class ConsumerRunnable. */
  public class ConsumerRunnable implements Runnable {

    /** The logger. */
    Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    /** The latch. */
    CountDownLatch latch;

    /** The consumer. */
    KafkaConsumer<String, String> consumer;

    /**
     * Instantiates a new consumer runnable.
     *
     * @param latch the latch
     * @param bootstrapServers the bootstrap servers
     * @param groupId the group id
     * @param topic the topic
     */
    public ConsumerRunnable(
        CountDownLatch latch, String bootstrapServers, String groupId, String topic) {
      this.latch = latch;

      // create consumer configs.
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      // create the consumer
      consumer = new KafkaConsumer<String, String>(properties);

      // subscribe consumer to topic(s)
      consumer.subscribe(Arrays.asList(topic));
    }

    /** Run. */
    public void run() {
      // poll for new data
      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String, String> record : records) {
            logger.info("Key: " + record.key() + ", Value: " + record.value());
            logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
          }
        }
      } catch (WakeupException e) {
        logger.info("Received  Shutdown signal!");
      } finally {
        consumer.close();
        // notifies main code that we are done.
        latch.countDown();
      }
    }

    /** Shutdown. */
    public void shutdown() {
      // the wapkeup() api is a special api to interrupt consumer.poll()
      // it will throw a WakeUpException.
      consumer.wakeup();
    }
  }
}
