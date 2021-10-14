<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="my.web.apps.db.*"%>

<%@ page import="java.io.ByteArrayInputStream"%>
<%@ page import="java.io.File"%>
<%@ page import="java.io.IOException"%>
<%@ page import="java.util.Arrays"%>
<%@ page import="java.util.Properties"%>
<%@ page import="org.apache.avro.Schema"%>
<%@ page import="org.apache.avro.generic.GenericDatumReader"%>
<%@ page import="org.apache.avro.generic.GenericRecord"%>
<%@ page import="org.apache.avro.io.BinaryDecoder"%>
<%@ page import="org.apache.avro.io.DatumReader"%>
<%@ page import="org.apache.avro.io.DecoderFactory"%>
<%@ page import="org.apache.kafka.clients.consumer.ConsumerConfig"%>
<%@ page import="org.apache.kafka.clients.consumer.ConsumerRecord"%>
<%@ page import="org.apache.kafka.clients.consumer.ConsumerRecords"%>
<%@ page import="org.apache.kafka.clients.consumer.KafkaConsumer"%>
<%@ page import="org.apache.kafka.common.serialization.ByteArrayDeserializer"%>
<%@ page import="org.apache.kafka.common.serialization.StringDeserializer"%>

<%@ page import="org.apache.avro.file.DataFileReader"%>

<%@ page import="org.apache.hadoop.fs.FileSystem"%>
<%@ page import="org.apache.hadoop.conf.Configuration"%>
<%@ page import="java.net.URI"%>
<%@ page import="org.apache.hadoop.fs.Path"%>
<%@ page import="org.apache.hadoop.util.Progressable"%>
<%@ page import="java.io.BufferedWriter"%>
<%@ page import="java.io.OutputStream"%>
<%@ page import="java.io.OutputStreamWriter"%>
<%@ page import="java.io.FileOutputStream"%>
<%@ page import="java.io.ObjectOutputStream"%>

<%@ page import="java.io.BufferedReader"%>
<%@ page import="java.io.InputStreamReader"%>
<%@ page import="java.util.Scanner"%>

<%@ page import="java.util.List"%>
<%@ page import="java.util.Arrays"%>

<!DOCTYPE html>
<html>
	<head>
		<meta charset="ISO-8859-1">
		<link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
		<script src="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.min.js"></script>
		<link rel="stylesheet" type="text/css" href="css/Page4.css">
		<title>Fourth Page</title>
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
		<div class="page4Row3">
			<h3>Press the button 'next' to see how many times each keyword appears</h3>
		</div>
		<br>
		<br>
		<form style="text-align:center;" action="Page5.jsp">
			<input type="submit" class="btn btn-primary" value="Next" name="submit">
		</form>
		<br>
		<br>
		<div class="page4Row1">
			<h3>All 500 tweets that have been deserialized</h3>
		</div>
		<br>
		<br>
		<%
			
			Properties props = new Properties();
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put("bootstrap.servers", "localhost:9092");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "AvroConsumer-GroupOne");
			
			Schema schema = new Schema.Parser().parse(new File("C:/Users/USER/Desktop/My_Avro_Twitter/schema/twitter.avsc"));
			
			KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
			kafkaConsumer.subscribe(Arrays.asList("my_tweets_topic"));
			
			String allTweets = "";
			int i = 0;
			int k = 0;
			while (true)
			{
				i++;
				ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(100);
				for (ConsumerRecord<String, byte[]> record : records)
				{
					k++;
					DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
					ByteArrayInputStream is = new ByteArrayInputStream(record.value());
					BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(is, null);
					if (k<500){
						allTweets += datumReader.read(null,  binaryDecoder) + "\n";
					}
					else{
						allTweets += datumReader.read(null,  binaryDecoder);
					}
				}
				if(i==2){
					break;
				}
			}
			
			Configuration configuration = new Configuration();
			FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
			Path file = new Path("hdfs://localhost:9000/BigDataTweets/2_Deserialized/Tweets.txt");
			if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
			OutputStream os = hdfs.create(file);
			BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
			br.write(allTweets);
			br.close();
			
			
			String[] lines = allTweets.split(System.getProperty("line.separator"));
			
			int j = 0;
			
			Scanner scanner = new Scanner(allTweets);
			while (scanner.hasNextLine()) {
				j++;
			  	String line = scanner.nextLine();
		    %>
		    <div class="page4Row2">
				<div>
					<h3><b><%out.println(j); %>)</b></h4>
					<br>
					<h4> <%out.println(line); %></h4>
				</div>
			</div>
			<br>
			<br>
		    <% 
			}
			scanner.close();
		    %>
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