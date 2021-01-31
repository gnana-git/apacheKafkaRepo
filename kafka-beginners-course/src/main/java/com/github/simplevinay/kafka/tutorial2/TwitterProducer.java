package com.github.simplevinay.kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	String consumerKey = "rMknOosy3xxCkoYnOsGgxkhah";
	String consumerSecret = "F0I3IvRnOvp9L8wvbLGFe64HbMafVPeUUNDfRdfCBKYrOpL5km";
	String token = "2169656605-rbxZW7qFdnKbt0Vd7Jp7HXpqShK7WRSqvGkOlYl";
	String secret = "FHreEEYsG2GdGUvHu5fAR7cz9ybPiS7mlpAVuuQ8umtzn";

	public TwitterProducer() {

	}

	public static void main(String[] args) {

		new TwitterProducer().run();

	}

	public void run() {

		logger.info("Setup");
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// create a twitter client
		Client hosebirdClient = createTwitterClient(msgQueue);

		// Attempts to establish a connection.
		hosebirdClient.connect();

		// create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		//add a shutdown hook
		Runtime.getRuntime().addShutdownHook( new Thread(() ->  {
			logger.info("stopping application........");
			logger.info("shutting down client from twitter......");
			hosebirdClient.stop();
			logger.info("closing producer.........");
			producer.close();
			logger.info("Done!");

		}));

		// loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		while (!hosebirdClient.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				hosebirdClient.stop();
			}

			if (msg != null) {
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception!=null) {
							logger.error("Something went wrong", exception);
						}
						
					}
				});
			}

		}
		logger.info("End of application");

	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		List<String> terms = Lists.newArrayList("bitcoin");

		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();

		return hosebirdClient;

	}

	private KafkaProducer<String, String> createKafkaProducer() {

		String bootstrapServers = "127.0.0.1:9092";

		// create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create the Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		return producer;
	}

}