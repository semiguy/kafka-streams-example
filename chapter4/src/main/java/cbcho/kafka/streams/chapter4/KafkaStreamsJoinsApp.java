package cbcho.kafka.streams.chapter4;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import cbcho.kafka.streams.chapter4.model.Purchase;
import cbcho.kafka.streams.chapter4.util.serde.StreamsSerdes;

public class KafkaStreamsJoinsApp {
	
	public static void main(String[] args) {
		
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());
		StreamsBuilder builder = new StreamsBuilder();
		
		Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
		Serde<String> stringSerde = Serdes.String();
		
		KeyValueMapper<String, Purchase, KeyValue<String, Purchase>> custIdCCMasking = (k, v) -> {
			
			Purchase masked = Purchase.builder(v).maskCreditCard().build();
			return new KeyValue<>(masked.getCustomerId(), masked);
		};
	}
	
	private static Properties getProperties() {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join_driver_client");
		
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_application");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
				
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.77.129:9092");	// 172.16.2.223:9092
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
		
		props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);
		
		return props;
	}
}
