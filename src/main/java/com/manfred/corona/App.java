package com.manfred.corona;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	SparkSession spark = SparkSession
    		    .builder()
    		    .config("spark.master", "local")
    		    .appName("Java Spark SQL Example")
    		    .getOrCreate();
    	
    	StructType userSchema = new StructType().add("_c60", "string").add("age", "string");
    	
    	Dataset<Row> newsStream = spark.readStream().schema(userSchema).option("", "\t").csv("src/main/resources/");
    	
    	/*Dataset<Row> df = spark.read()
    			 .option("delimiter", "\t")
    			 .csv("src/main/resources/20200322133000.export.CSV");*/
    	
    	newsStream.dropDuplicates("_c60");
    	
    	newsStream.createOrReplaceTempView("news");
    	
    	Dataset<Row> sqlDF = spark.sql("SELECT * FROM news where _c60 like '%coronavirus%' ");
    	
    	 StreamingQuery query = sqlDF.writeStream()
    		      .outputMode("complete")
    		      .format("console")
    		      .start();

    		    try {
					query.awaitTermination();
				} catch (StreamingQueryException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    	
    	
  
    
    	
    	
    }
}
