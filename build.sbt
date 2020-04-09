name := "drone-project"

version := "0.1"

scalaVersion := "2.12.11"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.1"

// https://mvnrepository.com/artifact/com.google.code.gson/gson
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.6"

libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0-preview2"

libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"


