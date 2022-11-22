package cbcho.kafka.streams.chapter2.producer;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import cbcho.kafka.streams.chapter2.model.PurchaseKey;
import cbcho.kafka.streams.chapter2.partitioner.PurchaseKeyPartitioner;

/*
 * 토픽 생성
 * bin/kafka-topics.sh --create --bootstrap-server 172.16.2.223:9092 --topic some-topic
 */

public class SimpleProducer {
	
	public static void main(String[] args) {
		
		Properties properties = new Properties();
		
		// common
		properties.put("bootstrap.servers", "172.16.2.223:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "3");
        properties.put("compression.type", "snappy");
        
        // add.
        properties.put("partitioner.class", PurchaseKeyPartitioner.class.getName());
        
        PurchaseKey key = new PurchaseKey("12334568", new Date());
		
        try(Producer<PurchaseKey, String> producer = new KafkaProducer<>(properties)) {
			
        	ProducerRecord<PurchaseKey, String> record = 
        			new ProducerRecord<PurchaseKey, String>("some-topic", key, "value");
        	
        	Callback callback = (metadata, exception) -> {
        		
        		if(exception != null) {
        			exception.printStackTrace();
        		}
        	};
        	
        	Future<RecordMetadata> sendFuture = producer.send(record, callback);
        	
		} catch (Exception e) {
			
			e.printStackTrace();
		}
        
	}
}
