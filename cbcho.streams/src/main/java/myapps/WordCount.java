package myapps;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;

/*
 * mvn clean package
 * mvn exec:java -Dexec.mainClass=myapps.WordCount
 * 
 * bin/kafka-console-producer.sh --broker-list 172.16.2.223:9092 --topic test-input-topic
 */
/*
 	bin/kafka-console-consumer.sh --bootstrap-server 172.16.2.223:9092 \
 	--topic test-output-topic \
 	--formatter kafka.tools.DefaultMessageFormatter \
 	--property print.key=true \
 	--property print.value=true \
 	--property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 	--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 */
public class WordCount {
	
	public static void main(String[] args) {
		
		// 설정
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.2.223:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		// 토폴로지 구성
		final StreamsBuilder builder = new StreamsBuilder();
		
		builder.<String, String>stream("test-input-topic")
			.flatMapValues(new ValueMapper<String, Iterable<String>>() {

				@Override
				public Iterable<String> apply(String value) {
					
					return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
				}
			})
			.groupBy(new KeyValueMapper<String, String, String>() {

				@Override
				public String apply(String key, String value) {
					
					return value;
				}
			})
			.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
			.toStream()
			.to("test-output-topic", Produced.with(Serdes.String(), Serdes.Long())); 
		
	    final Topology topology = builder.build();
	    System.out.println(topology.describe());
	    
	    final KafkaStreams streams = new KafkaStreams(topology, props);
	    final CountDownLatch latch = new CountDownLatch(1);
	    
	    // Ctrl+C를 처리하기 위한 핸들러
	    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			
	    	@Override
			public void run() {
				//super.run();
				streams.close();
				latch.countDown();
				System.out.println("topology terminated");
			}
	    });
	    
	    try {
	    	streams.start();
	    	System.out.println("topology started");
	    	latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
	    System.exit(0); 
	}
}
