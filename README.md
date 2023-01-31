## Twitter Sentiment Analysis
This a real-time sentiment analyser of tweets. I've used kafka to stream real-time tweets of a particular using twitter api and then process it through Spark to store it in MySQL table and display on frontend app built on Streamlit.

Steps to follow:
1. Clone the Repository.
2. Create a twitter developer account to get the bearer token replace it in Kafka streaming code.
3. Create a table in MySQL to store the data and set the table credentials in Spark code and frontend code.
4. Use the commands to start kafka server
    <br>kafka_2.12-3.2.1/bin/zookeeper-server-start.sh kafka_2.12-3.2.1/config/zookeeper.properties
    kafka_2.12-3.2.1/bin/kafka-server-start.sh kafka_2.12-3.2.1/config/server.properties
5. Create the kafka topic<br>
    kafka_2.12-3.2.1/bin/kafka-topics -zookeeper localhost:9092 -topic twitter -create
6. Start the kafka producer to stream tweets<br> 
    kafka_2.12-3.2.1/bin/kafka-console-producer.sh --topic twitter --bootstrap-server localhost:9092
7. Run the FilteredStream.java to start consuming from kafka producer.
8. Run the SparkKafkaConsumer.java to start processing from Kafka Filtered Stream and store it in MySQL.
9. Stream run twitter-streamlit.py to display the tweets on frontend.
