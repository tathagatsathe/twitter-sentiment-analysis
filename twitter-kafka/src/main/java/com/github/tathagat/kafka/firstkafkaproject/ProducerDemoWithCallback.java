package com.github.tathagat.kafka.firstkafkaproject;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;


public class ProducerDemoWithCallback {
	
	public static String CsvFile = "test.csv";
	
	private Producer<String, String> ProducerProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    
        return new KafkaProducer<String, String>(properties);
	}
	
    public static void main(String[] args) throws URISyntaxException{

//        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        ProducerDemoWithCallback kafkaProducer = new ProducerDemoWithCallback();
        kafkaProducer.PublishMessages();
//        Properties props = new Properties();
//        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
//        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
//        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        
//        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
//        
//        ProducerRecord<String, String> record = new ProducerRecord<String,String>("twitter", "Helloo World");
        System.out.println("Producing job completed");
//        producer.close();
//		producer.flush();
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//        
//        try {
//			URI uri = getClass().getClassLoader().getResource(CsvFile).toURI();
//			Stream<String> FileStream = Files.lines(Paths.get(uri));
//		} catch (URISyntaxException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//        ProducerRecord<String, String> record = new ProducerRecord<String,String>("twitter", "Hello World");
//
//        for (int i=0; i<10; i++){
//            ProducerRecord<String, String> record = new ProducerRecord<>("twitter", "Hello World");
//
//            producer.send(record, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if(e==null) {
//                        logger.info("Received new metadata.\n" +
//                                "Topic: "+recordMetadata.topic()+"\n"+
//                                "Partition: "+recordMetadata.partition() + "\n"+
//                                "Offset: "+recordMetadata.offset() + "\n"+
//                                "Timestamp: "+recordMetadata.timestamp());
//                    } else {
//                        logger.error("Error while producing ", e);
//                    }
//                }
//            });
//        }
//
//        producer.flush();
//        producer.close();
//        Logger logger = LoggerFactory.getLogger(ProducerDemo. class);
//        logger.info("This is");
    }

    private void PublishMessages() throws URISyntaxException {
    	final Producer<String, String> csvProducer = ProducerProperties();
    	
		try {
			URI uri = getClass().getClassLoader().getResource(CsvFile).toURI();
			Stream<String> FileStream = Files.lines(Paths.get(uri));
			
			FileStream.forEach(line -> {
				System.out.println(line);
				
				final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(
						"twitter",UUID.randomUUID().toString(),line);
				
				System.out.println(csvRecord.value());
				csvProducer.send(csvRecord, (metadata,exception) -> {
					System.out.println(csvRecord);
					if(metadata != null) {
						System.out.println("CsvData: -> "+ csvRecord.key()+" | "+csvRecord.value());
					}
					else {
						System.out.println("Error Sending Csv Record  -> "+ csvRecord.key()+" | "+csvRecord.value());
					}
				});
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		csvProducer.flush();
		csvProducer.close();
    	
    }
}
