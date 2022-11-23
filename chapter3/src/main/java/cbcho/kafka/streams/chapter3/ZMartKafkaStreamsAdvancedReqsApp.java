package cbcho.kafka.streams.chapter3;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import cbcho.kafka.streams.chapter3.model.Purchase;
import cbcho.kafka.streams.chapter3.model.PurchasePattern;
import cbcho.kafka.streams.chapter3.model.RewardAccumulator;
import cbcho.kafka.streams.chapter3.service.SecurityDBService;
import cbcho.kafka.streams.chapter3.util.serde.StreamsSerdes;

public class ZMartKafkaStreamsAdvancedReqsApp {
	
	public static void main(String[] args) throws Exception {
		
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());
		
		Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
		Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
		Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
		
		Serde<String> stringSerde = Serdes.String();
		
		StreamsBuilder builder = new StreamsBuilder();
		
		// 소스와 첫 번째 프로세서를 만든다.
		KStream<String, Purchase> purchaseKStream = builder
				.stream("transactions", Consumed.with(stringSerde, purchaseSerde))
				.mapValues(p -> Purchase.builder(p).maskCreditCard().build());
		
		// PurchasePattern 프로세서를 만든다.
		KStream<String, PurchasePattern> patternKStream = purchaseKStream
				.mapValues(purchase -> PurchasePattern.builder(purchase).build());
		patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
		patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde)); 
		
		// RewardAccumulator 프로세서를 만든다.
		KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream
				.mapValues(purchase -> RewardAccumulator.builder(purchase).build());
		rewardsKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards")); 
		rewardsKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));
		
		
		// new requirement
		// KeyValueMapper는 구매 날짜를 추출하고 Long으로 변환한다.
		KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase.getPurchaseDate().getTime();
		
		// 하나의 구문으로 구매를 필터링하고 키를 선택한다.
		KStream<Long, Purchase> filteredKStream = purchaseKStream
				.filter((key, purchase) -> purchase.getPrice() > 5.00).selectKey(purchaseDateAsKey); 
		
		// 결과를 콘솔에 인쇄한다.
		filteredKStream.print(Printed.<Long, Purchase>toSysOut().withLabel("purchases"));
		// 결과를 카프카 토픽에 기록한다.
		filteredKStream.to("purchases", Produced.with(Serdes.Long(), purchaseSerde)); 
		
		// 스트림 나누기
		Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
		Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");
		
		// 반환된 배열의 예상되는 인덱스에 라벨을 붙인다.
		int coffee = 0;
		int electronics = 1;
		
		// brach를 호출해 2개의 스트림으로 나눈다.
		KStream<String, Purchase>[] kstreamByDept = purchaseKStream.branch(isCoffee, isElectronics);
		// 각 스트림의 결과에 토픽에 쓴다.
		kstreamByDept[coffee].to("coffee", Produced.with(stringSerde, purchaseSerde));
		// 결과를 카프카 토픽에 기록한다.
		kstreamByDept[electronics].to("electronics", Produced.with(stringSerde, purchaseSerde));
		
		// security Requirements to record transactions for certain employee
        ForeachAction<String, Purchase> purchaseForeachAction = (key, purchase) ->
                SecurityDBService.saveRecord(purchase.getPurchaseDate(), purchase.getEmployeeId(), purchase.getItemPurchased());
        
        purchaseKStream.filter((key, purchase) -> purchase.getEmployeeId().equals("000000")).foreach(purchaseForeachAction);
		
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamsConfig);
        System.out.println("ZMart Advanced Requirements Kafka Streams Application Started");
        
        kafkaStreams.start();
        Thread.sleep(65000);
        
        System.out.println("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
	}	
	
	private static Properties getProperties() {
		
		Properties props = new Properties();
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.77.129:9092"); // 172.16.2.223:9092
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
		
		return props;
	}
}
