package com.manfred.corona;



import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

public final class OtherExample {
  private static String INPUT_DIRECTORY = "src/main/resources/";

  public static void main(String[] args) throws Exception {
    System.out.println("Starting Job ");
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);


    SparkSession spark = SparkSession
      .builder()
      .config("spark.eventLog.enabled", "false")
      .config("spark.driver.memory", "2g")
      .config("spark.master", "local")
      .config("spark.executor.memory", "2g")
      .appName("StructuredStreamingAverage")
      .getOrCreate();

    StructType personSchema = new StructType();
    for(int i=1; i<=27;i++) {
    	personSchema = personSchema.add("field"+i,"string");
    }
                                    
    Dataset<Row> personStream = spark
      .readStream()
      .schema(personSchema)
      .option("delimiter", "\t")
      .csv(INPUT_DIRECTORY);
     
    personStream.createOrReplaceTempView("news");

    String sql = "SELECT * FROM news";
    Dataset<Row> result = spark.sql(sql);

    StreamingQuery query = result.writeStream()
      .outputMode(OutputMode.Append())
      .format("console")
      .foreach(new ForeachWriter<Row>() {
		
		@Override
		public void process(Row value) {
			System.out.println(value);
			
		}
		
		@Override
		public boolean open(long partitionId, long epochId) {
			// TODO Auto-generated method stub
			return true;
		}
		
		@Override
		public void close(Throwable errorOrNull) {
			// TODO Auto-generated method stub
			
		}
	})
      .start();
    
    

    query.awaitTermination();
  }
}