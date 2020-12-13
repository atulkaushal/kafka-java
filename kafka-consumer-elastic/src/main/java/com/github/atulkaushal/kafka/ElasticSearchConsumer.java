package com.github.atulkaushal.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

/**
 * The Class ElasticSearchConsumer.
 *
 * @author Atul
 */
public class ElasticSearchConsumer {

  static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

  /**
   * The main method.
   *
   * @param args the arguments
   * @throws IOException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, InterruptedException {

    // Client with Elastic search deployed on local.
    RestHighLevelClient client = createClient();

    // Client with Elastic search deployed on remote server with credentials.
    // RestHighLevelClient client = createClientWithCredentials();

    KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

    // poll for new data
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      logger.info("Total records: " + records.count());
      for (ConsumerRecord<String, String> record : records) {

        // 2 strategies for creating unique ids for every record to avoid duplicate data insertion
        // in case of consumer failure.
        // first approach:- Kafka generic Id
        // String id= record.topic()+record.partition()+record.offset();

        // Second approach:- using id from object itself (twitter feed specific)
        // Every tweet has an id associated with it which is also unique.

        String id = extractIdFromTweet(record.value());

        // Insert data into elastic search
        IndexRequest indexRequest =
            new IndexRequest("twitter")
                .source(record.value(), XContentType.JSON)
                .id(id); // id to make consumer idempotent as it won't affect any workflow or
        // usecase

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        // String id = indexResponse.getId();
        logger.info(indexResponse.getId()); // uF10WXYB0uHI-27whyUE, uV14WXYB0uHI-27wpSVF
        Thread.sleep(10); // introduce delay for debugging
      }
      logger.info("Committing offsets");
      // manually saving offsets.
      consumer.commitSync();
      logger.info("Offsets has been committed.");
      Thread.sleep(10000);
    }

    // close the client
    // client.close();
  }

  /**
   * Extract id from tweet.
   *
   * @param tweetJson the tweet json
   * @return the string
   */
  private static String extractIdFromTweet(String tweetJson) {
    return JsonParser.parseString(tweetJson)
        .getAsJsonObject()
        .get("data")
        .getAsJsonObject()
        .get("id")
        .getAsString();
  }

  /**
   * Creates the client.
   *
   * @return the rest high level client
   */
  public static RestHighLevelClient createClient() {
    String hostname = "localhost";

    RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 9200, "http"));

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  /**
   * Creates the client with credentials.
   *
   * @return the rest high level client
   */
  public static RestHighLevelClient createClientWithCredentials() {
    String hostname = "";
    String username = "";
    String password = "";

    // don't do if you run a local Elastic Search
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder =
        RestClient.builder(new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {

                  public HttpAsyncClientBuilder customizeHttpClient(
                      HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                  }
                });

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  public static KafkaConsumer<String, String> createConsumer(String topic) {
    final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    String groupId = "kafka-demo-elasticsearch";
    // String topic = "twitter_tweets";

    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disabling auto commit of offsets
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

    // create the consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    // subscribe consumer to topic(s)
    consumer.subscribe(Arrays.asList(topic));

    return consumer;
  }
}
