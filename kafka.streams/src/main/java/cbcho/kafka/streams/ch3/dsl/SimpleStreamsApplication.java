package cbcho.kafka.streams.ch3.dsl;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

/*
 * bin/kafka-topics.sh --create --bootstrap-server 172.16.2.223:9092 --partitions 3 --topic stream_log
 * 
 * bin/kafka-console-producer.sh --bootstrap-server 172.16.2.223:9092 --topic test-input-topic
 * bin/kafka-console-consumer.sh --bootstrap-server 172.16.2.223:9092 --topic test-output-topic --from-beginning
 */

public class SimpleStreamsApplication {
	
	// 스트림즈 애플리케이션은 애플리케이션 아이디(application.id)를 지정해야한다.
	// 애플리케이션 아이디 값을 기준으로 병렬처리하기 때문이다.
	private static String APPLICATION_NAME = "streams-application";
	
	// 스트림즈 애플리케이션과 연동할 카프카 클러스터 정보를 입력한다.
	private static String BOOTSTRAP_SERVERS = "172.16.2.223:9092";
	
	private static String STREAM_LOG = "test-input-topic"; // stream_log
	private static String STREAM_LOG_COPY = "test-output-topic"; // stream_log_copy
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		
		// 스트림 처리를 위해 메시지 키와 메시지 값의 역직렬화, 직렬화 방식을 지정한다.
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		// StreamBuilder는 스트림 토폴로지를 정의하기 위한 용도로 사용된다.
		StreamsBuilder builder = new StreamsBuilder();
		// 토픽으로부터 KStream 객체를 만들기 위해 StreamBuilder의 stream() 메서드를 사용
		KStream<String, String> streamLog = builder.stream(STREAM_LOG);
		// 토픽을 담은 KStream 객체를 다른 토픽으로 전송하기 위해 to() 메서드를 사용
		streamLog.to(STREAM_LOG_COPY);
		
		// StreamBuilder로 정의한 토폴로지에 대한 정보와 스트림즈 실행을 위한 기본 옵션을 
		// 파라미터로 KafkaStreams 인스턴스를 생성
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
		
	}
}
