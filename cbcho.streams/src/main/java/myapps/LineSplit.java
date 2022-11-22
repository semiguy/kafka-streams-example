package myapps;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

/*
 * mvn clean package
 * mvn exec:java -Dexec.mainClass=myapps.LineSplit
 * 
 * bin/kafka-console-consumer.sh --bootstrap-server 172.16.2.223:9092 --topic test-output-topic --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=false --property print.value=true --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 * bin/kafka-console-producer.sh --broker-list 172.16.2.223:9092 --topic test-input-topic
 */

public class LineSplit {
	
	public static void main(String[] args) {
		
		// 설정
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.2.223:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		// 토폴로지 구성
		final StreamsBuilder builder = new StreamsBuilder();
		
		/*
		KStream<String, String> source = builder.stream("test-input-topic");
		KStream<String, String> words = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {

			@Override
			public Iterable<String> apply(String value) {
				
				return Arrays.asList(value.split("\\W+")); // 공백 기준으로 분리후 array 리스트로 반환
			}
		});
		words.to("test-output-topic");
		*/
		// lambda 표현
//		KStream<String, String> source = builder.stream("test-input-topic");
//		KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")));
//		words.to("test-output-topic"); 
		KStream<String, String> source = builder.stream("test-input-topic");
		source.flatMapValues(value -> Arrays.asList(value.split("\\W+"))).to("test-output-topic"); 
		
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
