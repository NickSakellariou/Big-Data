<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="my.web.apps.db.*"%>

<%@ page import="java.io.ByteArrayInputStream"%>
<%@ page import="java.io.File"%>
<%@ page import="java.io.IOException"%>
<%@ page import="java.util.Arrays"%>
<%@ page import="java.util.Properties"%>

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

<%@ page import="static org.apache.spark.sql.functions.col"%>

<%@ page import="java.util.List"%>
<%@ page import="java.util.Arrays"%>
<%@ page import="org.apache.spark.SparkConf"%>
<%@ page import="org.apache.spark.api.java.JavaRDD"%>
<%@ page import="org.apache.spark.api.java.JavaSparkContext"%>
<%@ page import="org.apache.spark.api.java.JavaPairRDD"%>
<%@ page import="scala.Tuple2"%>

<%@ page import="org.apache.spark.api.java.*"%>
<%@ page import="org.apache.spark.api.java.function.Function"%>
<%@ page import="org.apache.spark.mllib.clustering.KMeans"%>
<%@ page import="org.apache.spark.mllib.clustering.KMeansModel"%>
<%@ page import="org.apache.spark.mllib.linalg.Vector"%>
<%@ page import="org.apache.spark.mllib.linalg.Vectors"%>
<%@ page import="org.apache.spark.SparkConf"%>

<!DOCTYPE html>
<html>
	<head>
		<meta charset="ISO-8859-1">
		<link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
		<script src="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.min.js"></script>
		<link rel="stylesheet" type="text/css" href="css/Page5.css">
		<title>Fifth Page</title>
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
		<div class="page5Row1">
			<h3>Statistics results :</h3>
		</div>
		<%
			
			int totalFootball = 0;
			int totalBasketball = 0;
			int totalTennis = 0;
			int totalVolleyball = 0;
			int totalBaseball = 0;
			int totalCricket = 0;
			int totalHockey = 0;
			
			SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]");
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/BigDataTweets/2_Deserialized/Tweets.txt");
			JavaRDD<String> countFootball = lines.filter(line -> line.contains ("football"));
			JavaRDD<String> countBasketball = lines.filter(line -> line.contains ("basketball"));
			JavaRDD<String> countVolleyball = lines.filter(line -> line.contains ("volleyball"));
			JavaRDD<String> countTennis = lines.filter(line -> line.contains ("tennis"));
			JavaRDD<String> countBaseball = lines.filter(line -> line.contains ("baseball"));
			JavaRDD<String> countCricket = lines.filter(line -> line.contains ("cricket"));
			JavaRDD<String> countHockey = lines.filter(line -> line.contains ("hockey"));
			
			for(String countFootbal:countFootball.collect()){
				totalFootball++;
	        }
			
			for(String countBasketbal:countBasketball.collect()){
				totalBasketball++;
	        }
			
			for(String countVolleybal:countVolleyball.collect()){
				totalVolleyball++;
	        }
			
			for(String countTenni:countTennis.collect()){
				totalTennis++;
	        }
			
			for(String countBasebal:countBaseball.collect()){
				totalBaseball++;
	        }
			
			for(String countCricke:countCricket.collect()){
				totalCricket++;
	        }
			
			for(String countHocke:countHockey.collect()){
				totalHockey++;
	        }
			
			String Football = "The keyword football appears " + totalFootball + " times";
			String Basketall = "The keyword basketall appears " + totalBasketball + " times";
			String Volleyball = "The keyword volleyball appears " + totalVolleyball + " times";
			String Tennis = "The keyword tennis appears " + totalTennis + " times";
			String Baseball = "The keyword baseball appears " + totalBaseball + " times";
			String Cricket = "The keyword cricket appears " + totalCricket + " times";
			String Hockey = "The keyword hockey appears " + totalHockey + " times";
			
			System.out.println(Football);
			System.out.println(Basketall);
			System.out.println(Volleyball);
			System.out.println(Tennis);
			System.out.println(Baseball);
			System.out.println(Cricket);
			System.out.println(Hockey);
					
			%>
			<div class="page5Row2">
				<div>
					<h4> <%out.println(Football); %></h4>
					<br>
					<h4> <%out.println(Basketall); %></h4>
					<br>
					<h4> <%out.println(Volleyball); %></h4>
					<br>
					<h4> <%out.println(Tennis); %></h4>
					<br>
					<h4> <%out.println(Baseball); %></h4>
					<br>
					<h4> <%out.println(Cricket); %></h4>
					<br>
					<h4> <%out.println(Hockey); %></h4>
					<br>
				</div>
			</div>
			<br>
			<br>
			<%
			
			String statistics = Football + "\n" + Basketall + "\n" + Volleyball + "\n" + Tennis + "\n" + Baseball + "\n" + Cricket + "\n" + Hockey;
			
			Configuration configuration = new Configuration();
			FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
			Path file = new Path("hdfs://localhost:9000/BigDataTweets/3_SparkResults/Statistics.txt");
			if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
			OutputStream os = hdfs.create(file);
			BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
			br.write(statistics);
			br.close();
			
		%>
		<br>
		<br>
		<br>
		<br>
		<div class="page5Row3">
			<h3>Press the button 'next' to implement the clustering algorithm KMeans on the data</h3>
		</div>
		<br>
		<br>
		<form style="text-align:center;" action="Page6.jsp">
			<input type="submit" class="btn btn-primary" value="Next" name="submit">
		</form>
		<br>
		<br>
		<br>
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