package com.github.atulkaushal.kafka.streams.util;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.redouane59.twitter.TwitterClient;
import com.github.redouane59.twitter.dto.stream.StreamRules.StreamMeta;
import com.github.redouane59.twitter.dto.stream.StreamRules.StreamRule;
import com.github.redouane59.twitter.dto.tweet.TweetV2;
import com.github.redouane59.twitter.dto.tweet.TweetV2.TweetData;
import com.github.redouane59.twitter.dto.user.UserPublicMetrics;
import com.github.redouane59.twitter.dto.user.UserV2;
import com.github.redouane59.twitter.dto.user.UserV2.UserData.Includes;

/**
 * The Class TwitterUtil.
 *
 * @author Atul
 */
public class TwitterUtil {

  Logger logger = LoggerFactory.getLogger(TwitterUtil.class.getName());

  /** The twitter client. */
  static TwitterClient twitterClient;

  public TwitterUtil() {
    getTwitterClient();
  }

  /**
   * Gets the twitter client.
   *
   * @return the twitter client
   */
  public TwitterClient getTwitterClient() {
    if (twitterClient == null) {
      twitterClient = new TwitterClient();
    }
    return twitterClient;
  }

  /**
   * Delete all filtered stream rules.
   *
   * @param twitterClient the twitter client
   * @param allRules the all rules
   */
  public void deleteAllFilteredStreamRules() {
    // Get all rules.
    List<StreamRule> allRules = getAllRules();
    if (allRules != null) {
      for (StreamRule streamRule : allRules) {
        deleteFilteredStreamRule(streamRule.getValue());
      }
    } else {
      logger.info("No rule found to delete.");
    }
  }

  /**
   * Delete filtered stream rule.
   *
   * @param twitterClient the twitter client
   * @param ruleValue the rule value
   */
  public void deleteFilteredStreamRule(String ruleValue) {
    StreamMeta streamMeta = getTwitterClient().deleteFilteredStreamRule(ruleValue);
    logger.info(streamMeta.getSummary().toString());
  }

  /**
   * Prints the all rules.
   *
   * @param allRules the all rules
   */
  public void printAllRules() {
    // Get all rules.
    List<StreamRule> allRules = getAllRules();
    if (allRules != null) {
      for (StreamRule rule : allRules) {
        logger.info(rule.getId());
        logger.info(rule.getTag() + "," + rule.getValue());
      }
    } else {
      logger.info("No rule found.");
    }
  }

  /**
   * Gets the all rules.
   *
   * @return the all rules
   */
  public List<StreamRule> getAllRules() {
    TwitterClient twitterClient = new TwitterClient();
    // Get all rules.
    return twitterClient.retrieveFilteredStreamRules();
  }

  public void addRules(Map<String, String> rules) {
    for (String key : rules.keySet()) {
      getTwitterClient().addFilteredStreamRule(key, rules.get(key));
      // twitterClient.addFilteredStreamRule("Kafka", "Rule for Kafka");
    }
  }

  /**
   * Read stream.
   *
   * @param twitterClient the twitter client
   */
  @SuppressWarnings("unchecked")
  public void readStream() {
    logger.info("Reading Stream:");
    getTwitterClient()
        .startFilteredStream(
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
                String jsonString = new com.google.gson.Gson().toJson(customTweet);
                logger.info(jsonString);
                /*logger.info(tweet.getData().toString());
                logger.info(tweet.getText());
                logger.info(tweet.getUser().getName());
                logger.info(tweet.getData().getTweetType().toString());
                logger.info("{}", tweet.getLikeCount());
                logger.info("{}", tweet.getCreatedAt());
                logger.info(tweet.getUser().getDisplayedName());*/
              }
            });
  }

  /**
   * Prints the user details.
   *
   * @param twitterClient the twitter client
   */
  public void printAccountDetails(String twitterHandle) {
    UserV2 user = twitterClient.getUserFromUserName(twitterHandle);

    logger.info("Twitter ID : " + user.getName());
    logger.info("Display Name: " + user.getDisplayedName());
    logger.info("Total Tweets: " + user.getTweetCount());
    logger.info("Account date: " + user.getDateOfCreation());
    logger.info("Total followers: " + user.getFollowersCount());
    logger.info("Total following: " + user.getFollowingCount());
    logger.info("Location : " + user.getLocation());
    logger.info("Language : " + user.getData().getLang() != null ? user.getData().getLang() : "");
    logger.info("Protected : " + user.getData().isProtectedAccount());
    logger.info("Verified : " + user.getData().isVerified());
    UserPublicMetrics metrics = user.getData().getPublicMetrics();
    logger.info("Listed count: " + metrics.getListedCount());
    logger.info("URL: " + user.getData().getUrl());
    logger.info("Profile Image URL: " + user.getData().getProfileImageUrl());
    Includes includes = user.getIncludes();
    if (includes != null) {
      TweetData[] tweetDataArr = includes.getTweets();
      for (TweetData tweetData : tweetDataArr) {
        logger.info(tweetData.getText());
      }
    }
  }
}
