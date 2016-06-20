package it.reti.spark.sentiment

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.cassandra



//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * This object stores processing DataFrames into correct HIVE tables
 */
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DataStorer(processingType: String) extends Serializable with Logging{
  
   
  
  //get sqlHIVE context and import methods for DataFrame/RDD conversion 
  private val sqlContextHIVE = ContextHandler.getSqlContextHIVE
  import sqlContextHIVE.implicits._
  
  //check output table creation
  val tableTweets = "tweets_processed_" + processingType
  val tableSentiment = "tweets_sentiment_"  + processingType
  
  
  
  //CASSANDRA
  
  val keyspaceCassandra = "test"

  
  
  
  /*
   * HIVEEEEEEEEEE!
   * 
   * sqlContextHIVE.sql("create table if not exists " 
   
                     + tableTweets    
                     + " (`tweet_id` bigint, `lang` string, `user_id` bigint, `user_name` string, `latitude` double ,`longitude` double, `text` string) stored as parquet"
                     )
  sqlContextHIVE.sql("create table if not exists " 
                     + tableSentiment 
                     + " (`tweet_id` bigint, `sentiment_value` double, `matched_words` bigint, `tweet_words` bigint, `confidency_value` double) stored as parquet"
                     )
  
  */
  
  
  
  //.................................................................................................................
  /**
   * method to store tweet infos into HIVE tweetTable
   * @param tweetDF: a DataFrame of elaborated tweets ready to be stored
   */
  def storeTweetsToHIVE (tweetDF: DataFrame) = {
        
        /*<<INFO>>*/  logInfo("Writing tweets into HIVE table:")
        tweetDF.show()
        tweetDF.write.mode(SaveMode.Append).saveAsTable(tableTweets)
        //tweetDF.write.format("orc").save(tableTweets)
    
  }//end storeTweetsToHIVE method //
  
  
 
  
    //.................................................................................................................
  /**
   * method to store sentiment infos into HIVE sentimentTable
   * @param sentimentDF: a DataFrame of elaborated sentiment values ready to be stored
   */
  def storeSentimentToHIVE (sentimentDF: DataFrame) = {
        
        /*<<INFO>>*/ logInfo("Writing sentiment results into HIVE table:") 
        sentimentDF.cache().show()
        sentimentDF.write.mode(SaveMode.Append).saveAsTable(tableSentiment)
        //sentimentDF.write.format("orc").save(tableSentiment)
    
  }//end storeSentimentToHIVE method //
  
  
  
  
    //.................................................................................................................
  /**
   * method to store tweet infos into CASSANDRA tableTweets
   * @param tweetDF: a DataFrame of elaborated tweets ready to be stored
   */
  def storeTweetsToCASSANDRA (tweetDF: DataFrame) = {
        
        /*<<INFO>>*/  logInfo("Writing tweets into CASSANDRA table:")
        tweetDF.show()
        tweetDF.write.format("org.apache.spark.sql.cassandra").option("table",tableTweets).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()
        
    
  }//end storeTweetsToCASSANDRA method //
  
  
 
  
    //.................................................................................................................
  /**
   * method to store sentiment infos into CASSANDRA tableSentiment
   * @param sentimentDF: a DataFrame of elaborated sentiment values ready to be stored
   */
  def storeSentimentToCASSANDRA (sentimentDF: DataFrame) = {
        
        /*<<INFO>>*/ logInfo("Writing sentiment results into CASSANDRA table:") 
        sentimentDF.cache().show()
        sentimentDF.write.format("org.apache.spark.sql.cassandra").option("table",tableSentiment).option("keyspace",keyspaceCassandra).mode(SaveMode.Append).save()

  }//end storeSentimentToCASSANDRA method //
  
  
}//end DataStorer class //