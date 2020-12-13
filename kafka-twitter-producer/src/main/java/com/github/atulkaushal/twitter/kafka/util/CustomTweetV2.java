package com.github.atulkaushal.twitter.kafka.util;

import com.github.redouane59.twitter.dto.tweet.TweetV2;
import com.github.redouane59.twitter.dto.user.User;

/**
 * The Class CustomTweetV2 was created to get user information within tweet data.
 *
 * @author Atul
 */
public class CustomTweetV2 extends TweetV2 {

  /** The user. */
  User user;

  /**
   * Gets the user.
   *
   * @return the user
   */
  public User getUser() {
    return user;
  }

  /**
   * Sets the user.
   *
   * @param user the new user
   */
  public void setUser(User user) {
    this.user = user;
  }

  /**
   * Sets the user by user id.
   *
   * @param userId the user id
   * @return the user
   */
  public User setUserByUserId(String userId) {
    return new TwitterUtil().getTwitterClient().getUserFromUserId(userId);
  }
}
