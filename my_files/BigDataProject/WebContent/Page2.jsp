<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="my.web.apps.db.*"%>

<%@ page import="com.twitter.bijection.Injection"%>
<%@ page import="com.twitter.bijection.avro.GenericAvroCodecs"%>
<%@ page import="org.apache.avro.Schema"%>
<%@ page import="org.apache.avro.generic.GenericData"%>
<%@ page import="org.apache.avro.generic.GenericRecord"%>
<%@ page import="org.apache.kafka.clients.producer.KafkaProducer"%>
<%@ page import="org.apache.kafka.clients.producer.ProducerRecord"%>
<%@ page import="java.io.File"%>
<%@ page import="java.io.IOException"%>
<%@ page import="java.util.Properties"%>

<%@ page import="java.util.concurrent.BlockingQueue"%>
<%@ page import="java.util.concurrent.LinkedBlockingQueue"%>
<%@ page import="com.google.common.collect.Lists"%>
<%@ page import="com.twitter.hbc.ClientBuilder"%>
<%@ page import="com.twitter.hbc.core.Client"%>
<%@ page import="com.twitter.hbc.core.Constants"%>
<%@ page import="com.twitter.hbc.core.endpoint.StatusesFilterEndpoint"%>
<%@ page import="com.twitter.hbc.core.processor.StringDelimitedProcessor"%>
<%@ page import="com.twitter.hbc.httpclient.auth.Authentication"%>
<%@ page import="com.twitter.hbc.httpclient.auth.OAuth1"%>

<%@ page import="twitter4j.JSONException"%>
<%@ page import="twitter4j.JSONObject"%>
<%@ page import="org.json.*"%>
<%@ page import="org.apache.kafka.clients.producer.Producer"%>

<!DOCTYPE html>
<html>
	<head>
		<meta charset="ISO-8859-1">
		<link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
		<script src="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.min.js"></script>
		<link rel="stylesheet" type="text/css" href="css/Page2.css">
		<title>Second Page</title>
	</head>
	<body>
		<div class="header">
		  	<br>
			<a href="Page1.jsp">
				<h1>
					Big Data Twitter
				</h1>
			</a>
			<br>
			<br>
		</div>
		<br>
		<br>
		<br>
		<br>
		<div class="page2Row3">
			<h3>Press the button 'next' to store the data at hdfs</h3>
		</div>
		<br>
		<br>
		<form style="text-align:center;" action="Page3.jsp">
			<input type="submit" class="btn btn-primary" value="Next" name="submit">
		</form>
		<br>
		<br>
		<div class="page2Row1">
			<h3>All 500 tweets that have been collected</h3>
		</div>
		<br>
		<br>
		<div>
			<%
			
				Properties props = new Properties();
				props.put("bootstrap.servers", "localhost:9092");
				props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
				
				Schema schema = new Schema.Parser().parse(new File("C:/Users/USER/Desktop/My_Avro_Twitter/schema/twitter.avsc"));
				
				Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
				
				KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
				
				String consumerKey = "";
				String consumerSecret = "";
				String token = "";
				String secret = "";
				BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
				StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
				endpoint.trackTerms(Lists.newArrayList("football,basketball,tennis,volleyball,baseball,cricket,hockey"));
				Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
				
				Client client = new ClientBuilder()
						.hosts(Constants.STREAM_HOST)
						.endpoint(endpoint)
						.authentication(auth)
						.processor(new StringDelimitedProcessor(queue))
						.build();
				
				client.connect();
				
				for (int i = 0; i < 500; i++)
				{
					
					String msg = queue.take();
				    JSONObject obj = new JSONObject(msg);
				    
				    String text= obj.getString("text");
				    int followers_count = obj.getJSONObject("user").getInt("followers_count");
				    int statuses_count = obj.getJSONObject("user").getInt("statuses_count");
				    
				    String keyword1  = "football";
				    String keyword2  = "basketball";
				    String keyword3  = "tennis";
				    String keyword4  = "volleyball";
				    String keyword5  = "baseball";
				    String keyword6  = "cricket";
				    String keyword7  = "hockey";
				    
				    if (text.indexOf(keyword1.toLowerCase()) == -1 && text.indexOf(keyword2.toLowerCase()) == -1 && text.indexOf(keyword3.toLowerCase()) == -1 && text.indexOf(keyword4.toLowerCase()) == -1 && text.indexOf(keyword5.toLowerCase()) == -1 && text.indexOf(keyword6.toLowerCase()) == -1 && text.indexOf(keyword7.toLowerCase()) == -1) {
				    	i--;
				    	continue;
				    }
				    
				    
				    System.out.println(text);
				    System.out.println(followers_count);
				    System.out.println(statuses_count);
					
					GenericData.Record avroRecord = new GenericData.Record(schema);
					avroRecord.put("text", text);
					avroRecord.put("followers_count", followers_count);
					avroRecord.put("statuses_count", statuses_count);
					
					%>
					<div class="page2Row2">
						<div>
							<h3><b><%out.println(i + 1); %>)</b></h4>
							<br>
							<h4>
								<b>Text : </b><%out.println(text); %>
								<br>
								<br>
								<b>Total followers of the account : </b><%out.println(followers_count); %>
								<br>
								<br>
								<b>Total Tweets the account has posted : </b><%out.println(statuses_count); %>
							</h4>
						</div>
					</div>
					<br>
					<br>
					<%
					
					byte[] bytes = recordInjection.apply(avroRecord);
					ProducerRecord<String, byte[]> record = new ProducerRecord<>("my_tweets_topic", bytes);	
					producer.send(record);
					System.out.println("Sent:" + record);
					
					//Thread.sleep(250);
				}
				
				producer.close();
				client.stop();
			%>
		
			<form style="text-align:center;" action="Page1.jsp">
				<input type="submit" class="btn btn-danger" value="Go back" name="submit">
			</form>
		
		</div>
		<br>
		<br>
		<div class="footer">
			&copy;
			<a href="http://www.ds.unipi.gr/" target="_blank">2021 DS_UNIPI
			</a>.
			All rights reserved.
		</div>
	</body>
</html>