package cbcho.kafka.streams.chapter3;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class KafkaStreamsYellingApp {
	
	public static void main(String[] args) throws Exception {
		
		Properties props = new Properties();
		// 카프카 스트림즈 프로그램을 설정하기 위한 속성
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.2.223:9092");
		
		// 주어진 속성으로 streamsConfig 생성
		StreamsConfig streamsConfig = new StreamsConfig(props);
		
		// 키와 값을 직렬화/역직렬화 하는 데 사용하는 Serdes 생성
		Serde<String> stringSerde = Serdes.String();
		
		// 프로세서 토폴로지를 구성하는 데 사용하는 StreamBuilder 인스턴스를 생성
		StreamsBuilder builder = new StreamsBuilder();
		
		// 그래프의 부모 노드에서 읽을 소스 토픽으로 실제 스트림 생성
		// 스트림 소스 정의
		 KStream<String, String> simpleFirstStream = builder.stream("test-input-topic", Consumed.with(stringSerde, stringSerde));
		 
		 // 자바 8 메소드 핸들(그래프의 첫 번째 자식 노드)을 사용한 프로세서
		 // 유입 텍스트를 대문자로 매핑
		 //KStream<String, String> upperCasedStream = simpleFirstStream.mapValues(String::toUpperCase);
		 KStream<String, String> upperCasedStream = simpleFirstStream.mapValues(v -> v.toUpperCase()); 
		 
		 // 변환된 결과를 다른 토픽(그래프의 싱크 노드)에 쓴다.
		 // 싱크 노드 생성
		 upperCasedStream.to("test-out-topic", Produced.with(stringSerde, stringSerde)); 
		 upperCasedStream.print(Printed.<String, String>toSysOut().withLabel("Yelling App"));
		 
		 KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
		 
		 System.out.println("Hello World Yelling App Started");
		 // 카프카 스트림즈 스레드 시작
		 kafkaStreams.start();
		 Thread.sleep(350000);
		 
		 System.out.println("Shutting down the Yelling APP now");
		 kafkaStreams.close();
	}
}
