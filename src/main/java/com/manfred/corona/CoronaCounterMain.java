package com.manfred.corona;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

public final class CoronaCounterMain {
	public static String INPUT_DIRECTORY = "src/main/resources/";
	public static String INPUT_DIRECTORY_DOWNLOAD = INPUT_DIRECTORY + "downloads";
	public static String INPUT_MASTER_DIRECTORY_FILE = "src/main/resources/master/masterfilelist-translation.txt";

	private static Map<Long, CoronaResult> coronaResults = new ConcurrentHashMap<Long, CoronaResult>();

	public static void main(String[] args) throws Exception {
		System.out.println("Starting Job ");
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		new Thread(new CoronaDownloadNews()).start();

		SparkSession spark = SparkSession.builder().config("spark.eventLog.enabled", "false")
				.config("spark.driver.memory", "3g").config("spark.master", "local")
				.config("spark.executor.memory", "3g").appName("StructuredStreamingAverage").getOrCreate();

		StructType newsSchema = new StructType();
		for (int i = 0; i <= 26; i++) {
			newsSchema = newsSchema.add("field" + i, "string");
		}

		Dataset<Row> newsStream = spark.read().schema(newsSchema).option("delimiter", "\t").csv(INPUT_DIRECTORY);

		newsStream.createOrReplaceTempView("news");

		/*String sql =  "SELECT count(*) "
				+ "FROM news where field7 like '%CORONAVIRUS%'";*/
		
		String sql = "SELECT avg(split(field15, ',')[0]) as avg, "
				+ "max(split(field15, ',')[0]) as max , "
				+ "min(split(field15, ',')[0]) as min, "
				+ "sum(split(field15, ',')[0])  as sum, "
				+ "field1 as currentDateAnalysis, "
				+ "count(*) as nrOfNews "
				+ "FROM news where field7 like '%TAX_DISEASE_CORONAVIRUS%' group by currentDateAnalysis order by currentDateAnalysis";
		
		
		final Dataset<Row> result = spark.sql(sql);
		
		result.coalesce(1).write().mode(SaveMode.Overwrite).csv("src/main/resources/result/file.resultCorona.csv");
	}

}