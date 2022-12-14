package cbcho.kafka.streams.chapter3;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import cbcho.kafka.streams.chapter3.model.Purchase;
import cbcho.kafka.streams.chapter3.model.PurchasePattern;
import cbcho.kafka.streams.chapter3.model.RewardAccumulator;
import cbcho.kafka.streams.chapter3.util.serde.StreamsSerdes;

public class ZMartKafkaStreamsApp {

	public static void main(String[] args) throws Exception {
		
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());
		
		Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
		Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
		Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
		
		Serde<String> stringSerde = Serdes.String();
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		
		// 소스와 첫 번째 프로세서를 만든다.
		KStream<String, Purchase> purchaseKStream = streamsBuilder
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
		
		// 스토리지 싱크를 만든다. 토픽은 스토리지 컨슈머가 사용한다.
		purchaseKStream.print(Printed.<String, Purchase>toSysOut().withLabel("purchases"));
		purchaseKStream.to("purchases", Produced.with(stringSerde, purchaseSerde)); 
		
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
		
		System.out.println("ZMart First Kafka Streams Application Started");
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
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.2.223:9092");
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
		
		return props;
	}
}
