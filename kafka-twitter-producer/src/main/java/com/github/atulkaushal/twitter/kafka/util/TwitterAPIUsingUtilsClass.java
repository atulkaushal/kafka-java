package com.github.atulkaushal.twitter.kafka.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.redouane59.twitter.dto.stream.StreamRules.StreamRule;

/**
 * The Class TwitterAPI.
 *
 * @author Atul
 */
public class TwitterAPIUsingUtilsClass {

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

    TwitterUtil twitterUtil = new TwitterUtil();

    // print account details
    twitterUtil.printAccountDetails("twitterdev");

    // Get all rules.
    List<StreamRule> allRules = twitterUtil.getAllRules();

    // print all rules.
    twitterUtil.printAllRules();

    // Delete a single rule by mentioning rule value
    twitterUtil.deleteFilteredStreamRule("Rule for Java");

    // delete all filter rules.
    twitterUtil.deleteAllFilteredStreamRules();

    // Create rule for filteredStream by providing value and tag.
    Map<String, String> rules = new HashMap<String, String>();
    rules.put("Java", "Rule for Java");
    rules.put("#100DaysOfCode", "Rule for 100 days of code");
    rules.put("Bitcoin", "Rule for Bitcoin");
    twitterUtil.addRules(rules);

    twitterUtil.readStream();
  }
}
