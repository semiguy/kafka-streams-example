package cbcho.kafka.streams.ch3.dsl;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

/*
 * bin/kafka-topics.sh --create --bootstrap-server 172.16.2.223:9092 --partitions 2 --topic address_v2
 */

public class KStreamJoinGlobalKTable {
	
	private static String APPLICATION_NAME = "global-table-join-application";
	private static String BOOTSTRAP_SERVERS = "172.16.2.223:9092";
	
	private static String ADDRESS_GLOBAL_TABLE = "address_v2";
	private static String ORDER_STREAM = "order";
	private static String ORDER_JOIN_STREAM = "order_join";
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		GlobalKTable<String, String> addressGlobalTable = builder.globalTable(ADDRESS_GLOBAL_TABLE);
		KStream<String, String> orderStream = builder.stream(ORDER_STREAM);
		
		orderStream.join(addressGlobalTable, 
				(orderKey, orderValue) -> orderKey, 
				(order, address) -> order + " sebd to " + address)
				.to(ORDER_JOIN_STREAM);
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		
		streams.start();
	}
	
}
