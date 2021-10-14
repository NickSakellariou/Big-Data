<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="my.web.apps.db.*"%>
<%@ page import="java.io.*"%>
<!DOCTYPE html>
<html>
	<head>
		<meta charset="ISO-8859-1">
		<link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
		<script src="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.min.js"></script>
		<link rel="stylesheet" type="text/css" href="css/Page3.css">
		<title>Third Page</title>
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
		<div class="page3Row1">
			<h3>User needs to type these two commands in order to continue :</h3>
			<br>
			<h3>cd C:\apache-flume-1.9.0-bin</h3>
			<br>
			<h3>bin\flume-ng agent --conf .\conf -f conf\twit.conf -property "flume.root.logger=info,console" -n tier1</h3>
		</div>
		<%
			CmdOpener op = new CmdOpener();
			op.open();
		%>
		<br>
		<br>
		<br>
		<br>
		<div class="page3Row2">
			<h3>Press the button 'next' to deserialize data and store them to HDFS</h3>
		</div>
		<br>
		<br>
		<form style="text-align:center;" action="Page4.jsp">
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