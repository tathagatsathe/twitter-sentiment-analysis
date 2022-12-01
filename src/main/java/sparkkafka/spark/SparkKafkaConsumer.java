package sparkkafka.spark;
import java.io.FileInputStream;
//import java.io.FileWriter;
import java.io.IOException;
//import java.io.StringReader;
//import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
//import javafx.util.Pair;\
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import kafka.serializer.StringDecoder;
import com.google.gson.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Connection;
import java.util.AbstractMap;
//import edu.stanford.nlp.;

public class SparkKafkaConsumer {
public static void main(String[] args) throws IOException {
		
		System.out.println("Spark Streaming started now .....");
		System.out.println(System.getProperty("java.version"));
		
		String configFilePath = "src/main/java/sparkkafka/spark/config.properties";
		FileInputStream propsInput = new FileInputStream(configFilePath);
		Properties prop = new Properties();
		try {
			prop.load(propsInput);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String host = prop.getProperty("host");
		String user = prop.getProperty("user");
		String password = prop.getProperty("password");
		
		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		kafkaParams.put("auto.offset.reset", "largest");
		Set<String> topics = Collections.singleton("twitter");
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection(host,user,password);
			directKafkaStream.foreachRDD(rdd -> {
				  System.out.println("Records");  
				  System.out.println("New data arrived  " + rdd.partitions().size() +" Partitions and " + rdd.count() + " Records");
					  if(rdd.count() > 0) {
						rdd.collect().forEach(rawRecord -> {
							  
							  System.out.println("***************************************");
							  System.out.println(rawRecord._2());
							  Gson gson = new Gson();
							  if(!rawRecord._2().isEmpty()) {
								  JsonObject record = JsonParser.parseString(rawRecord._2()).getAsJsonObject();
								  if(record.isJsonObject()) {
									  JsonElement data = record.get("data");
									  if(data.isJsonObject()) {
										  JsonObject dataObj = data.getAsJsonObject();
										  JsonElement text = dataObj.get("text");
										  long id = dataObj.get("id").getAsLong();
										  System.out.println("TEXT: "+text);
										  Sentiment sentimentAnalyzer = new Sentiment();
										  sentimentAnalyzer.init(); 
										  AbstractMap.SimpleEntry<Integer, String> p = sentimentAnalyzer.findSentiment(text.toString());
										  System.out.println(java.time.LocalDateTime.now());
										  String query = "INSERT INTO tweets(ID,text,sentimentInt,sentimentName, created) values (?,?,?,?,?)";
										  PreparedStatement stmt;
										try {
											stmt = con.prepareStatement(query);
											stmt.setLong(1, id);
											stmt.setString(2,text.toString());
											stmt.setInt(3,p.getKey());
											stmt.setString(4,p.getValue());
											stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
											stmt.executeUpdate();
										} catch (SQLException e) {
											e.printStackTrace();
										}
									  }
								  }
							  }
							  
						  });
					  }
				  });
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		ssc.start();
		ssc.awaitTermination();
	}
}
