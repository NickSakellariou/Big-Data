<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
	<head>
		<meta charset="ISO-8859-1">
		<link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
		<script src="http://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.min.js"></script>
		<link rel="stylesheet" type="text/css" href="css/Page1.css">
		<title>First Page</title>
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
		<div class="page1Row1">
			<h3>Press the button 'search' to see 500 tweets that contain at least one of these keywords : football, basketball, tennis, volleyball, baseball, cricket, hockey</h3>
		</div>
		<br>
		<br>
		<form style="text-align:center;" action="Page2.jsp">
			<input type="submit" class="btn btn-primary" value="Search" name="submit">
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