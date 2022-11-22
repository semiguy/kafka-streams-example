package cbcho.kafka.streams.ch3.processapi;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/*
 * bin/kafka-console-producer.sh --bootstrap-server 172.16.2.223:9092 --topic test-input-topic
 * bin/kafka-console-consumer.sh --bootstrap-server 172.16.2.223:9092 --topic test-output-topic
 */

public class SimpleKafkaProcessor {
	
	private static String APPLICATION_NAME = "processor-application";
	private static String BOOTSTRAP_SERVERS = "172.16.2.223:9092";
	
	private static String STREAM_LOG = "test-input-topic"; // stream_log
	private static String STREAM_LOG_FILTER = "test-output-topic"; // stream_log_copy
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		// Topology 클래스는 프로세서 API를 사용한 토폴로지를 구성하기 위해 사용
		Topology topology = new Topology();
		
		// 토픽을 소스 프로세서로 가져오기 위해 addSource() 메서드를 사용
		// addSource()메서드의 첫 번째 파라미터에는 소스 프로세서의 이름을 입력하고 두 번째 파라미터는 대상 토픽 이름을 입력한다.
		topology.addSource("Source", STREAM_LOG)
				// 스트림 프로세서를 사용하기 위해 addProcessor() 메서드를 사용했다.
				// addProcessor() 메서드의 첫 번째 파라미터는 스트림 프로세서의 이름을 입력한다.
				// 두 번째 파라미터는 사용자가 정의한 프로세서 인스턴스를 입력한다.
				// 세 번째 파라미터는 부모 노드를 입력
				.addProcessor("Process", 
						() -> new FilterProcessor(), 
						"Source")
				// 싱크 프로세서로 사용하여 데이터를 저장하기 위해 addSink() 메서드를 사용
				// 첫 번째 파라미터는 싱크 프로세서의 이름을 입력한다.
				// 두 번째 파라미터는 저장할 토픽의 이름을 입력 한다.
				// 세 번째는 부모 노드를 입력
				.addSink("Sink", 
						STREAM_LOG_FILTER, 
						"Process");
		
		// 작성 완료한 topology 인스턴스를 KafkaStreams 인스턴스의 파라미터로 넣어서 스트림즈를 생성하고 실행
		KafkaStreams streaming = new KafkaStreams(topology, props);
		streaming.start();
	}
}
