package com.github.atulkaushal.twitter.kafka;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.atulkaushal.twitter.kafka.util.CustomTweetV2;
import com.github.atulkaushal.twitter.kafka.util.TwitterUtil;
import com.github.redouane59.twitter.TwitterClient;
import com.github.redouane59.twitter.dto.tweet.TweetV2;

/**
 * The Class TwitterProducer.
 *
 * @author Atul
 */
public class TwitterProducer {

  /** The logger. */
  static Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

  /** The Constant BOOTSTRAP_SERVERS. */
  private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

  /** Instantiates a new twitter producer. */
  TwitterProducer() {}

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  /** Run. */
  public void run() {

    // create twitter client
    TwitterClient twitterClient = createTwitterClient();

    // create kafka producer
    KafkaProducer<String, String> producer = createKafkaProducer();

    // push data into kafka
    pushDataToKafka(twitterClient, producer);
  }

  /**
   * Push data to kafka.
   *
   * @param twitterClient the twitter client
   * @param producer the producer
   */
  @SuppressWarnings("unchecked")
  private static void pushDataToKafka(
      TwitterClient twitterClient, KafkaProducer<String, String> producer) {
    System.out.println("Reading Stream:");
    twitterClient.startFilteredStream(
        new Consumer() {
          @Override
          public void accept(Object t) {
            TweetV2 tweet = (TweetV2) t;
            CustomTweetV2 customTweet = new CustomTweetV2();
            try {
              BeanUtils.copyProperties(customTweet, tweet);
            } catch (IllegalAccessException | InvocationTargetException e) {
              e.printStackTrace();
            }
            customTweet.setUser(customTweet.setUserByUserId(customTweet.getAuthorId()));
            String jsonTweet = new com.google.gson.Gson().toJson(customTweet);
            logger.info(jsonTweet);
            producer.send(
                new ProducerRecord<String, String>("twitter_tweets", jsonTweet),
                new Callback() {

                  @Override
                  public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                      logger.error("Something went wrong!", exception);
                    }
                  }
                });
          }
        });
  }

  /**
   * Creates the kafka producer.
   *
   * @return the kafka producer
   */
  private KafkaProducer<String, String> createKafkaProducer() {
    // Create Producer Properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // add properties for safe producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

    // no need to set "retries" property as default value is already Integer.MAX_VALUE
    // no need to set "max.in.flight.requests.per.connection" property as its already 5

    // High throughput producer (at the expense of a bit of latency and CPU usage)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

    // Create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    return producer;
  }

  /**
   * Creates the twitter client.
   *
   * @return the twitter client
   */
  private TwitterClient createTwitterClient() {
    // delete all filter rules.
    TwitterUtil util = new TwitterUtil();
    util.deleteAllFilteredStreamRules();
    // create new rules
    TwitterClient twitterClient = new TwitterClient();
    twitterClient.addFilteredStreamRule("Kafka", "Rule for Kafka");
    twitterClient.addFilteredStreamRule("Java", "Rule for Java");
    twitterClient.addFilteredStreamRule("#100DaysOfCode", "Rule for 100 days of code");
    twitterClient.addFilteredStreamRule("Bitcoin", "Rule for Bitcoin");
    twitterClient.addFilteredStreamRule("Soccer", "Rule for Soccer");
    twitterClient.addFilteredStreamRule("politics", "Rule for politics");
    twitterClient.addFilteredStreamRule("Sports", "Rule for Sports");
    twitterClient.addFilteredStreamRule("USA", "Rule for usa");
    return twitterClient;
  }
}
