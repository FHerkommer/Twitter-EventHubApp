package com.org.simulation;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class SendTweetsToEventHub {

	// Replace values below with real configurations
	final private static String twitterConsumerKey = "Hbn0ISayL2SZKu8hn7VKXWoII";
	final private static String twitterConsumerSecret = "FJcWqAeGZ0fTnAJfd2NfOAW98qE0WwhToB74yM1yfjhCxAVGjH";
	final private static String twitterOauthAccessToken = "1322874566960709636-lb6RbAQgutNd4Ck5AI5ewjRBopXVx1";
	final private static String twitterOauthTokenSecret = "2iGnXulknfSjrxV6wh3pA38ycLZvnJ1HwJixKITNzycxl";
	
	final private static String namespaceName = "twitter-eventhub";
	final private static String eventHubName = "twitter-eventhub";
	final private static String sasKeyName = "manage";
	final private static String sasKey = "bbtEQLj7+ukD0wrllNCURPrsQBN+val4g6euQaDEdQY=";
	private static CompletableFuture<EventHubClient> eventHubClient;

	public static void main(String[] args) 
			throws TwitterException, EventHubException, IOException, InterruptedException, ExecutionException {

		 // EventHub configuration!
		ConnectionStringBuilder connStr = new ConnectionStringBuilder()
				.setNamespaceName(namespaceName)
				.setEventHubName(eventHubName)
				.setSasKeyName(sasKeyName)
				.setSasKey(sasKey);

		ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);
		eventHubClient = EventHubClient.create(connStr.toString(), pool);


		 // Twitter configuration!
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(twitterConsumerKey)
		.setOAuthConsumerSecret(twitterConsumerSecret)
		.setOAuthAccessToken(twitterOauthAccessToken)
		.setOAuthAccessTokenSecret(twitterOauthTokenSecret);

		TwitterFactory twitterFactory = new TwitterFactory(cb.build());
		Twitter twitter = twitterFactory.getInstance();

		// Getting tweets with keyword "Azure" and sending them to the Event Hub in realtime!
		Query query = new Query(" #Azure ");
		Query query1 = new Query(" Microsoft" );
		query.setCount(100);
		query1.setCount(100);
		query.lang("en");
		query1.lang("en");
		boolean finished = false;
		while (!finished) {
			QueryResult result = twitter.search(query);
			QueryResult result2 = twitter.search(query1);
			List<Status> statuses = result.getTweets();
			List<Status> statuses2 = result2.getTweets();
			long lowestStatusId = Long.MAX_VALUE;
			for(Status status:statuses){
				if(!status.isRetweet()){
					sendEvent(status.getText(), 500);
				}
				lowestStatusId = Math.min(status.getId(), lowestStatusId);
			}
			for(Status status:statuses2){
				if(!status.isRetweet()){
					sendEvent(status.getText(), 500);
				}
				lowestStatusId = Math.min(status.getId(), lowestStatusId);
			}
			query.setMaxId(lowestStatusId - 1);
		}

	}

	private static void sendEvent(String message, long delay) throws InterruptedException, UnsupportedEncodingException, ExecutionException {
		Thread.sleep(delay);
		EventData messageData = EventData.create(message.getBytes(StandardCharsets.UTF_8));
		eventHubClient.get().send(messageData);
		System.out.println("Sent event: " + message + "\n");

	}

}
