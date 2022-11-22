package cbcho.kafka.streams.ch3.dsl;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

/*
 * bin/kafka-console-producer.sh --bootstrap-server 172.16.2.223:9092 --topic test-input-topic
 * bin/kafka-console-consumer.sh --bootstrap-server 172.16.2.223:9092 --topic test-output-topic --from-beginning
 */

public class StreamsFilter {
	
	// 스트림즈 애플리케이션은 애플리케이션 아이디(application.id)를 지정해야한다.
	// 애플리케이션 아이디 값을 기준으로 병렬처리하기 때문이다.
	private static String APPLICATION_NAME = "streams-filter-application";
	
	// 스트림즈 애플리케이션과 연동할 카프카 클러스터 정보를 입력한다.
	private static String BOOTSTRAP_SERVERS = "172.16.2.223:9092";
	
	private static String STREAM_LOG = "test-input-topic"; // stream_log
	private static String STREAM_LOG_COPY = "test-output-topic"; // stream_log_copy
	private static String STREAM_LOG_FILTER = "test-output-topic"; // stream_log_filter
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		
		// 스트림 처리를 위해 메시지 키와 메시지 값의 역직렬화, 직렬화 방식을 지정한다.
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		// stream_log 토픽을 가져오기 위한 소스 프로세서를 작성.
		KStream<String, String> streamLog = builder.stream(STREAM_LOG);
		// 데이터를 필터링하는 filter() 메서드는 자바의 함수형 인터페이스인 Predicate를 파라미터로 받음
		KStream<String, String> filteredStream = streamLog.filter(
				(key, value) -> value.length() > 5);
		filteredStream.to(STREAM_LOG_FILTER);
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
}
