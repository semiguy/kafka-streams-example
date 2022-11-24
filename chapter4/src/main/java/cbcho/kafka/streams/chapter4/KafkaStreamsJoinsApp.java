package cbcho.kafka.streams.chapter4;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueJoiner;

import cbcho.kafka.streams.chapter4.joiner.PurchaseJoiner;
import cbcho.kafka.streams.chapter4.model.CorrelatedPurchase;
import cbcho.kafka.streams.chapter4.model.Purchase;
import cbcho.kafka.streams.chapter4.util.serde.StreamsSerdes;

public class KafkaStreamsJoinsApp {
	
	public static void main(String[] args) throws Exception {
		
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());
		StreamsBuilder builder = new StreamsBuilder();
		
		Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
		Serde<String> stringSerde = Serdes.String();
		
		KeyValueMapper<String, Purchase, KeyValue<String, Purchase>> custIdCCMasking = (k, v) -> {
			
			Purchase masked = Purchase.builder(v).maskCreditCard().build();
			return new KeyValue<>(masked.getCustomerId(), masked);
		};
		
		Predicate<String, Purchase> coffeePurchase = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("cofee");
		// 레코드 매치를 위한 predicate 정의하기
		Predicate<String, Purchase> electronicPurchase = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");
		
		// 일치하는 배열에 접근할 때 명확히 하기 위해 라벨이 있는 정수를 사용
		int COFFEE_PURCHASE = 0;
        int ELECTRONICS_PURCHASE = 1;
        
        KStream<String, Purchase> transactionStream = builder.stream("transactions", Consumed.with(Serdes.String(), purchaseSerde)).map(custIdCCMasking);
        
        // 분기된 스트림 생성
        // SelectKey 처리 노드를 삽입
        KStream<String, Purchase>[] branchesStream = transactionStream.selectKey((k, v) -> v.getCustomerId()).branch(coffeePurchase, electronicPurchase);
        
        // 분기된 스트림 추출
        KStream<String, Purchase> coffeeStream = branchesStream[COFFEE_PURCHASE];
        KStream<String, Purchase> electronicsStream = branchesStream[ELECTRONICS_PURCHASE];
        
        // 조인을 수행하는 데 사용하는 ValueJoiner 인스턴스
        ValueJoiner<Purchase, Purchase, CorrelatedPurchase> purchaseJoiner = new PurchaseJoiner();
        
        JoinWindows twentyMinuteWindow = JoinWindows.of(60 * 1000 * 20);
        
        // join 메소드를 호출해 coffeeStream 과 electronicsStream의 자동 리파티셔닝을 작동 시킨다.
        KStream<String, CorrelatedPurchase> joinedKStream = coffeeStream
        		.join(electronicsStream, purchaseJoiner, twentyMinuteWindow, 
        				Joined.with(stringSerde, purchaseSerde, purchaseSerde)); // 조인을 구성한다.
        
        joinedKStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("joinedKStream"));
        
        System.out.println("Starting Join Examples");
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.start();
        
        Thread.sleep(65000);
        
        System.out.println("Shutting down the Join Examples now");
        kafkaStreams.close();
        
        
	}
	
	private static Properties getProperties() {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join_driver_client");
		
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_application");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
				
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.2.223:9092");	// 172.16.2.223:9092, 192.168.77.129:9092
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
		
		props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);
		
		return props;
	}
}
