package cbcho.kafka.streams.chapter4;

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
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import cbcho.kafka.streams.chapter4.model.Purchase;
import cbcho.kafka.streams.chapter4.model.PurchasePattern;
import cbcho.kafka.streams.chapter4.model.RewardAccumulator;
import cbcho.kafka.streams.chapter4.partitioner.RewardsStreamPartitioner;
import cbcho.kafka.streams.chapter4.transformer.PurchaseRewardTransformer;
import cbcho.kafka.streams.chapter4.util.serde.StreamsSerdes;

public class ZMartKafkaStreamsAddStateApp {
	
	public static void main(String[] args) throws Exception {
		
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());
		
		Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();
        
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, Purchase> purchaseKStream = builder
        		.stream("transactions", Consumed.with(stringSerde, purchaseSerde))
        		.mapValues(p -> Purchase.builder(p).maskCreditCard().build());
        
        KStream<String, PurchasePattern> patternKStream = purchaseKStream
        		.mapValues(purchase -> PurchasePattern.builder(purchase).build());
        patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
        patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde)); 
        
        // adding State to processor
        String rewardsStateStoreName = "rewardsPointsStore";
        
        // StreamPartitioner를 구현한 인스턴스를 초기화
        RewardsStreamPartitioner streamPartitioner = new RewardsStreamPartitioner();
        
        // 상태 저장소 추가하기
        // StateStore 공급자를 생성한다.
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);
        // StoreBuilder를 생성하고 키와 값의 타입을 명시한다.
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores
        		.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());
        // 상태 저장소를 토폴로지에 추가한다.
        builder.addStateStore(storeBuilder);
        
        // KStream.though로 KStream을 생성한다.
        KStream<String, Purchase> transByCustomerStream = purchaseKStream
        		.through("customer_transactions", Produced.with(stringSerde, purchaseSerde, streamPartitioner));
        
        // 상태를 가진 변환을 사용하기 위해 보상 프로세서 변경하기
        KStream<String, RewardAccumulator> statefulRewardAccumulator = transByCustomerStream
        		.transformValues(() -> new PurchaseRewardTransformer(rewardsStateStoreName), rewardsStateStoreName);
        
        statefulRewardAccumulator.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards"));
        // 결과를 토픽에 기록한다.
        statefulRewardAccumulator.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde)); 
        
        System.out.println("Starting Adding State Example");
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        
        System.out.println("ZMart Adding State Application Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        
        Thread.sleep(65000);
        System.out.println("Shutting down the Add State Application now");
        kafkaStreams.close();
		
	}
	
	private static Properties getProperties() {
		
		Properties props = new Properties();
		
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "AddingStateConsumer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "AddingStateGroupId");
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AddingStateAppId");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.2.223:9092");	// 172.16.2.223:9092, 192.168.77.129:9092
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
		
		return props;
	}
}
