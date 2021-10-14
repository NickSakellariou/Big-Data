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
		<link rel="stylesheet" type="text/css" href="css/Page7.css">
		<title>Seventh Page</title>
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
		<div class="page7Row1">
			<h3>Clustering algorithm KMeans results results :</h3>
		</div>
		<%
			
		SparkConf conf = new SparkConf().setAppName("Kmeans").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> data = sc.textFile("hdfs://localhost:9000/BigDataTweets/4_Preprocessing/Data.txt");
		JavaRDD<Vector> parsedData = data.map(
			new Function<String, Vector>() {
				public Vector call(String s) {
					String[] sarray = s.split(" ");
					double[] values = new double[sarray.length];
					for (int i = 0; i < sarray.length; i++)
						values[i] = Double.parseDouble(sarray[i]);
					return Vectors.dense(values);
				}
			}
		);
		parsedData.cache();
		
		int numClusters = 2;
		int numIterations = 10;
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

		Vector[] centers = clusters.clusterCenters();
		System.out.println("Cluster centers:");
		for (Vector center: centers) {
			 %>
		    <div class="page7Row2">
				<div>
					<h3><b>Cluster centers:</b></h4>
					<br>
					<h4> <%out.println(center); %></h4>
				</div>
			</div>
			<br>
			<br>
		    <% 
			System.out.println(center);
		}
		
		System.out.println("Points cluster ID: " +clusters.predict(parsedData).collect());
		
		clusters.save(sc.sc(), "hdfs://localhost:9000/BigDataTweets/5_KMeansResults");
		KMeansModel sameModel = KMeansModel.load(sc.sc(), "hdfs://localhost:9000/BigDataTweets/5_KMeansResults");
			
		%>
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