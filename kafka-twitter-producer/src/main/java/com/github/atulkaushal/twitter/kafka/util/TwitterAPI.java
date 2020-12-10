package com.github.atulkaushal.twitter.kafka.util;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.redouane59.twitter.TwitterClient;
import com.github.redouane59.twitter.dto.stream.StreamRules.StreamMeta;
import com.github.redouane59.twitter.dto.stream.StreamRules.StreamRule;
import com.github.redouane59.twitter.dto.tweet.TweetV2;
import com.github.redouane59.twitter.dto.tweet.TweetV2.TweetData;
import com.github.redouane59.twitter.dto.user.UserPublicMetrics;
import com.github.redouane59.twitter.dto.user.UserV2;
import com.github.redouane59.twitter.dto.user.UserV2.UserData.Includes;

/**
 * The Class TwitterAPI.
 *
 * @author Atul
 */
public class TwitterAPI {

  /**
   * The main method.
   *
   * @param args the arguments
   * @throws JsonParseException the json parse exception
   * @throws JsonMappingException the json mapping exception
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void main(String[] args)
      throws JsonParseException, JsonMappingException, IOException {

    TwitterClient twitterClient = new TwitterClient();

    printUserDetails(twitterClient);

    // Get all rules.
    List<StreamRule> allRules = twitterClient.retrieveFilteredStreamRules();

    // print all rules.
    printAllRules(allRules);

    // Delete a single rule by mentioning rule value
    // deleteFilteredStreamRule(twitterClient, "Rule for Java");

    // delete all filter rules.
    deleteAllFilteredStreamRules(twitterClient, allRules);

    // Create rule for filteredStream by providing value and tag.
    twitterClient.addFilteredStreamRule("Java", "Rule for Java");
    twitterClient.addFilteredStreamRule("#100DaysOfCode", "Rule for 100 days of code");
    twitterClient.addFilteredStreamRule("Bitcoin", "Rule for Bitcoin");

    readStream(twitterClient);
  }

  /**
   * Read stream.
   *
   * @param twitterClient the twitter client
   */
  @SuppressWarnings("unchecked")
  private static void readStream(TwitterClient twitterClient) {
    System.out.println("Reading Stream:");
    twitterClient.startFilteredStream(
        new Consumer() {

          @Override
          public void accept(Object t) {
            TweetV2 tweet = (TweetV2) t;
            String jsonString = new com.google.gson.Gson().toJson(tweet);
            System.out.println(jsonString);
            System.out.println(tweet.getData().toString());
            System.out.println(tweet.getText());
            System.out.println(tweet.getData().getTweetType());
            System.out.println(tweet.getLikeCount());
            System.out.println(tweet.getCreatedAt());
            System.out.println(tweet.getUser().getDisplayedName());
          }
        });
  }

  /**
   * Delete all filtered stream rules.
   *
   * @param twitterClient the twitter client
   * @param allRules the all rules
   */
  private static void deleteAllFilteredStreamRules(
      TwitterClient twitterClient, List<StreamRule> allRules) {
    if (allRules != null) {
      for (StreamRule streamRule : allRules) {
        deleteFilteredStreamRule(twitterClient, streamRule.getValue());
      }
    } else {
      System.out.println("No rule found to delete.");
    }
  }

  /**
   * Delete filtered stream rule.
   *
   * @param twitterClient the twitter client
   * @param ruleValue the rule value
   */
  private static void deleteFilteredStreamRule(TwitterClient twitterClient, String ruleValue) {
    StreamMeta streamMeta = twitterClient.deleteFilteredStreamRule(ruleValue);
    System.out.println(streamMeta.getSummary());
  }

  /**
   * Prints the all rules.
   *
   * @param allRules the all rules
   */
  private static void printAllRules(List<StreamRule> allRules) {
    if (allRules != null) {
      for (StreamRule rule : allRules) {
        System.out.println(rule.getId());
        System.out.println(rule.getTag() + "," + rule.getValue());
      }
    } else {
      System.out.println("No rule found.");
    }
  }

  /**
   * Prints the user details.
   *
   * @param twitterClient the twitter client
   */
  private static void printUserDetails(TwitterClient twitterClient) {
    UserV2 user = twitterClient.getUserFromUserName("twitterdev");

    System.out.println("Twitter ID : " + user.getName());
    System.out.println("Display Name: " + user.getDisplayedName());
    System.out.println("Total Tweets: " + user.getTweetCount());
    System.out.println("Account date: " + user.getDateOfCreation());
    System.out.println("Total followers: " + user.getFollowersCount());
    System.out.println("Total following: " + user.getFollowingCount());
    System.out.println("Location : " + user.getLocation());
    System.out.println(
        "Language : " + user.getData().getLang() != null ? user.getData().getLang() : "");
    System.out.println("Protected : " + user.getData().isProtectedAccount());
    System.out.println("Verified : " + user.getData().isVerified());
    UserPublicMetrics metrics = user.getData().getPublicMetrics();
    System.out.println("Listed count: " + metrics.getListedCount());
    System.out.println("URL: " + user.getData().getUrl());
    System.out.println("Profile Image URL: " + user.getData().getProfileImageUrl());
    Includes includes = user.getIncludes();
    if (includes != null) {
      TweetData[] tweetDataArr = includes.getTweets();
      for (TweetData tweetData : tweetDataArr) {
        System.out.println(tweetData.getText());
      }
    }
  }
}
