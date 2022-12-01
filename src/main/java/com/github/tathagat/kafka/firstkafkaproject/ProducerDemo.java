package com.github.tathagat.kafka.firstkafkaproject;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


public class ProducerDemo {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
    	
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStructuredStreamingWithKafka");
//		SparkSession spark = SparkSession
//				  .builder()
//				  .appName("JavaStructuredNetworkWordCount")
//				  .getOrCreate();
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
//		Dataset<Row> df = spark
//				  .readStream()
//				  .format("kafka")
//				  .option("kafka.bootstrap.servers", "127.0.0.1:9092")
//				  .option("subscribe", "twitter")
//				  .option("startingOffsets", "earliest")
//				  .load();
		
		Dataset<Row> df = spark
				  .readStream()
				  .format("socket")
				  .option("host", "localhost")
				  .option("port", "9092")
				  .option("startingOffsets", "earliest")
				  .load();
//		spark.sparkContext().setLogLevel("ERROR");
//	    Dataset<Row> df = spark
//				.readStream()
//				.format("kafka")
//				.option("kafka.bootstrap.servers", "127.0.0.1:9092")
//				.option("subscribe", "twitter")
//				.load();
					df
				  .selectExpr("value")
				  .writeStream()
				  .format("console")
				  .option("checkpointLocation", "chk-point-dir")
				  .start();
//				  .format("kafka")
//				  .option("kafka.bootstrap.servers", "127.0.0.1:9092")
//				  .option("topic", "twitter")
//				  .option("checkpointLocation", "checkpoint")
//				  .start();
//		spark.sql("SELECT * FROM df").show();
//		StreamingQuery query = df.writeStream().format("console").start();
//		query.awaitTermination();
//		query = df.writeStream.format("console").start()
		
		
//    	df.printSchema();
//    	System.out.println(df);
//    	String configFilePath = "src/main/java/com/github/tathagat/kafka/firstkafkaproject/config.properties";
//    	FileInputStream propsInput = new FileInputStream(configFilePath);
//        Properties prop = new Properties();
//        try {
//			prop.load(propsInput);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//        
////        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
//        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        
//        System.out.println(prop);
//        
//        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
//
//        ProducerRecord<String, String> record = new ProducerRecord<>("twitter","Hello World");
//
//        producer.send(record);
//
//        producer.flush();
//        producer.close();
//        Logger logger = LoggerFactory.getLogger(ProducerDemo. class);
//        logger.info("This is");
    }
    
//    final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
//
//    private Client client;
//    private KafkaProducer<String, String> producer;
//    private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(30);
//    private List<String> trackTerms = Lists.newArrayList("coronavirus");
//    
//    public Client createTwitterClient(BlockingQueue<String> msgQueue){
//        /** Setting up a connection   */
//        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
//        StatusesFilterEndpoint hbEndpoint = new StatusesFilterEndpoint();
//        // Term that I want to search on Twitter
//        hbEndpoint.trackTerms(trackTerms);
//        // Twitter API and tokens
//        Authentication hosebirdAuth = new OAuth1(TwitterConfig.CONSUMER_KEYS, TwitterConfig.CONSUMER_SECRETS, TwitterConfig.TOKEN, TwitterConfig.SECRET);
//        /** Creating a client   */
//        ClientBuilder builder = new ClientBuilder()
//                .name("Hosebird-Client")
//                .hosts(hosebirdHosts)
//                .authentication(hosebirdAuth)
//                .endpoint(hbEndpoint)
//                .processor(new StringDelimitedProcessor(msgQueue));
//
//        Client hbClient = builder.build();
//        return hbClient;
//} 
//    <dependency>
//    <groupId>org.slf4j</groupId>
//    <artifactId>slf4j-jdk14</artifactId>
//    <version>1.7.25</version>
//</dependency>
}
