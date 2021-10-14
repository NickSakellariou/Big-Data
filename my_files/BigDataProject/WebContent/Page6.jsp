<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="my.web.apps.db.*"%>

<%@ page import="java.io.*"%>
<%@ page import="java.util.*"%>
<%@ page import="java.net.*"%>

<%@ page import="org.apache.hadoop.fs.FileSystem"%>
<%@ page import="org.apache.hadoop.conf.Configuration"%>
<%@ page import="java.net.URI"%>
<%@ page import="org.apache.hadoop.fs.*"%>
<%@ page import="org.apache.hadoop.util.Progressable"%>
<%@ page import="org.apache.hadoop.conf.Configuration"%>

<%@ page import="static org.apache.spark.sql.functions.col"%>

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

<%@ page import="org.apache.commons.io.IOUtils"%>

<%@ page import="org.json.simple.JSONObject"%>
<%@ page import="org.json.simple.parser.JSONParser"%>
<%@ page import="org.json.simple.parser.ParseException"%>

<!DOCTYPE html>
<html>
	<head>
		<meta charset="ISO-8859-1">
		<link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
		<script src="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.min.js"></script>
		<link rel="stylesheet" type="text/css" href="css/Page6.css">
		<title>Sixth Page</title>
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
		<div class="page6Row2">
			<h3>Press the button 'next' to implement the clustering algorithm KMeans using MLlib on the preprocessed data</h3>
		</div>
		<br>
		<br>
		<form style="text-align:center;" action="Page7.jsp">
			<input type="submit" class="btn btn-primary" value="Next" name="submit">
		</form>
		<br>
		<br>
		<br>
		<br>
		<div class="page6Row1">
			<h3>First we preprocess the data and store them to hdfs</h3>
		</div>
		<br>
		<br>
		<div class="page6Row1">
			<h3>1 means the text contained the word football</h3>
			<h3>2 means the text contained the word basketball</h3>
			<h3>3 means the text contained the word tennis</h3>
			<h3>4 means the text contained the word volleyball</h3>
			<h3>5 means the text contained the word baseball</h3>
			<h3>6 means the text contained the word cricket</h3>
			<h3>7 means the text contained the word hockey</h3>
		</div>
		<br>
		<br>
		<%
			
		Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        //Create a path
        String fileName = "Tweets.txt";
        Path hdfsReadPath = new Path("/BigDataTweets/2_Deserialized/" + fileName);
        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
        String out2= IOUtils.toString(inputStream, "UTF-8");
        
        String[] lines = out2.split(System.getProperty("line.separator"));
		
		int j = 0;
		
		Configuration configuration2 = new Configuration();
		FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
		Path file = new Path("hdfs://localhost:9000/BigDataTweets/4_Preprocessing/Data.txt");
		if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
		OutputStream os = hdfs.create(file);
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		
		String keyword1  = "football";
	    String keyword2  = "basketball";
	    String keyword3  = "tennis";
	    String keyword4  = "volleyball";
	    String keyword5  = "baseball";
	    String keyword6  = "cricket";
	    String keyword7  = "hockey";
	    
	    int sport = 0;
		
		Scanner scanner = new Scanner(out2);
		while (scanner.hasNextLine()) {
			j++;
		  	String line = scanner.nextLine();
		  	
		  	JSONParser jsonParser = new JSONParser();
            try {
               JSONObject jsonObject = (JSONObject) jsonParser.parse(line);
               String text = (String) jsonObject.get("text");
               long followers_count = (long) jsonObject.get("followers_count");
               long statuses_count = (long) jsonObject.get("statuses_count");
               System.out.println("Contents of the JSON are: ");
               System.out.println("Text :"+text);
               System.out.println("Followers_count : "+followers_count);
               System.out.println("Statuses count : "+statuses_count);
               System.out.println(" ");
               
               if (text.indexOf(keyword1) != -1) {
            	   sport = 1;
               }
               else if(text.indexOf(keyword2) != -1){               
            	   sport = 2;
               }
               else if(text.indexOf(keyword3) != -1){               
            	   sport = 3;
               }
               else if(text.indexOf(keyword4) != -1){               
            	   sport = 4;
               }
               else if(text.indexOf(keyword5) != -1){               
            	   sport = 5;
               }
               else if(text.indexOf(keyword6) != -1){               
            	   sport = 6;
               }
               else if(text.indexOf(keyword7) != -1){               
            	   sport = 7;
               }
               
               %>
	   		    <div class="page6Row3">
	   				<div>
	   					<h3><b><%out.println(j); %>)</b></h4>
	   					<br>
	   					<h4>Sport : <%out.println(sport); %></h4>
	   					<br>
	   					<h4>Followers count : <%out.println(followers_count); %></h4>
	   					<br>
	   					<h4>Statuses count : : <%out.println(statuses_count); %></h4>
	   				</div>
	   			</div>
	   			<br>
	   			<br>
	   		    <% 
               
               if(j<500) {
            	   br.write(sport + " " + followers_count + " " + statuses_count);
            	   br.newLine();
               }
               else {
            	   br.write(sport + " " + followers_count + " " + statuses_count);
               }
            } catch (ParseException e) {
               e.printStackTrace();
            }
		}
		scanner.close();
		br.close();
        
        inputStream.close();
        fileSystem.close();
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