package cbcho.kafka.streams.ch3.dsl;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

/*
 * bin/kafka-topics.sh --create --bootstrap-server 172.16.2.223:9092 --partitions 3 --topic address
 * bin/kafka-topics.sh --create --bootstrap-server 172.16.2.223:9092 --partitions 3 --topic order
 * bin/kafka-topics.sh --create --bootstrap-server 172.16.2.223:9092 --partitions 3 --topic order_join
 * 
 * bin/kafka-console-producer.sh --bootstrap-server 172.16.2.223:9092 --topic address --property "parse.key=true" --property "key.separator=:"
 * bin/kafka-console-producer.sh --bootstrap-server 172.16.2.223:9092 --topic order --property "parse.key=true" --property "key.separator=:"
 * 
 * bin/kafka-console-consumer.sh --bootstrap-server 172.16.2.223:9092 --topic order_join --property print.key=true --property key.separator=":" --from-beginning
 */

public class KStreamJoinKTable {
	
	private static String APPLICATION_NAME = "order-join-application";
	private static String BOOTSTRAP_SERVERS = "172.16.2.223:9092";
	
	private static String ADDRESS_TABLE = "address";
	private static String ORDER_STREAM = "order";
	private static String ORDER_JOIN_STREAM = "order_join";
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		// address 토픽을 KTable로 가져올 때는 table() 메서드를 소스 프로세서로 사용.
		KTable<String, String> addressTable = builder.table(ADDRESS_TABLE);
		// order 토픽은 KStream으로 가져올 것이므로 stream() 메서드를 소스 프로세서로 사용.
		KStream<String, String> orderStream = builder.stream(ORDER_STREAM);
		
		// 조인을 위해 KStream 인스턴스에 정의되어 있는 join 메서드를 사용
		// 첫 번째 파라미터로 조인을 수행할 KTable 인스턴스를 넣는다.
		orderStream.join(addressTable, 
				(order, address) -> order + " send to " + address)
				.to(ORDER_JOIN_STREAM);
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
}
