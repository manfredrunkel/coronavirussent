package com.manfred.corona;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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

		//new Thread(new CoronaDownloadNews()).start();

		SparkSession spark = SparkSession.builder().config("spark.eventLog.enabled", "false")
				.config("spark.driver.memory", "2g").config("spark.master", "local")
				.config("spark.executor.memory", "2g").appName("StructuredStreamingAverage").getOrCreate();

		StructType newsSchema = new StructType();
		for (int i = 0; i <= 26; i++) {
			newsSchema = newsSchema.add("field" + i, "string");
		}

		Dataset<Row> newsStream = spark.readStream().schema(newsSchema).option("delimiter", "\t").csv(INPUT_DIRECTORY);

		newsStream.createOrReplaceTempView("news");
		
		String sql = "SELECT avg(split(field15, ',')[0]) as tone, field1 as currentDateAnalysis "
				+ "FROM news where field7 like '%TAX_DISEASE_CORONAVIRUS%' group by currentDateAnalysis";
		final Dataset<Row> result = spark.sql(sql);

		/*String sql = "SELECT avg(split(field15, ',')[0]) as tone, field1 as currentDateAnalysis "
				+ "FROM news where field4 like '%corona%' group by currentDateAnalysis";
		*/

		StreamingQuery query = result.writeStream().outputMode(OutputMode.Complete()).foreach(new ForeachWriter<Row>() {
			@Override
			public void process(Row value) {
				double tone = (Double) value.get(0);
				long timestamp = Long.valueOf((String) value.get(1));
				CoronaResult coronaResult = new CoronaResult();
				if (coronaResults.containsKey(timestamp)) {
					coronaResult = coronaResults.get(timestamp);
				}
				coronaResult.setTimestamp(timestamp);
				coronaResult.setTone(tone);
				//coronaResult.setNewsTotalCoronaVirus(value.getLong(2));
				coronaResults.put(timestamp, coronaResult);
				System.out.println(coronaResult);
			}

			@Override
			public boolean open(long partitionId, long epochId) {
				return true;
			}

			@Override
			public void close(Throwable errorOrNull) {

			}
		}).start();
		
		query.awaitTermination();
	}

}