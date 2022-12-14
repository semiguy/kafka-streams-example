package cbcho.kafka.streams.chapter5;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import cbcho.kafka.streams.chapter5.clients.producer.MockDataProducer;
import cbcho.kafka.streams.chapter5.collectors.FixedSizePriorityQueue;
import cbcho.kafka.streams.chapter5.model.ShareVolume;
import cbcho.kafka.streams.chapter5.model.StockTransaction;
import cbcho.kafka.streams.chapter5.util.serde.StreamsSerdes;

public class AggregationsAndReducingExample {
	
	private static final String STOCK_TRANSACTIONS_TOPIC = "stock-transactions";
	
	public static void main(String[] args) throws Exception {
		
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());
		
		Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<ShareVolume> shareVolumeSerde = StreamsSerdes.ShareVolumeSerde();
        Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = StreamsSerdes.FixedSizePriorityQueueSerde();
        NumberFormat numberFormat = NumberFormat.getInstance();
        
        Comparator<ShareVolume> comparator = (sv1, sv2) -> sv2.getShares() - sv1.getShares();
        FixedSizePriorityQueue<ShareVolume> fixedQueue = new FixedSizePriorityQueue<ShareVolume>(comparator, 5);
        
        ValueMapper<FixedSizePriorityQueue, String> valueMapper = fpq -> {
        	
        	StringBuilder builder = new StringBuilder();
        	Iterator<ShareVolume> iterator = fpq.iterator();
        	int counter = 1;
        	while(iterator.hasNext()) {
        		ShareVolume stockVolume = iterator.next();
        		if(stockVolume != null) {
        			builder.append(counter++).append(")").append(stockVolume.getSymbol())
        					.append(":").append(numberFormat.format(stockVolume.getShares())).append(" ");
        		}
        	}
        	return builder.toString();
        };
        
		StreamsBuilder builder = new StreamsBuilder();
		
		KTable<String, ShareVolume> shareVolume = builder.stream(STOCK_TRANSACTIONS_TOPIC, 
				Consumed.with(stringSerde, stockTransactionSerde).withOffsetResetPolicy(AutoOffsetReset.EARLIEST)) // ?????? ????????? ???????????? ????????????
				.mapValues(st -> ShareVolume.newBuilder(st).build()) // StockTransaction ????????? ShareVolume ????????? ??????
				.groupBy((k, v) -> v.getSymbol(), Serialized.with(stringSerde, shareVolumeSerde)) // ?????? ?????? ????????? ?????? ShareVolume ????????? ?????????
				.reduce(ShareVolume::sum); // ???????????? ???????????? ?????? ShareVolume ?????? ?????????
		
		shareVolume.groupBy((k, v) -> KeyValue.pair(v.getIndustry(), v), 
				Serialized.with(stringSerde, shareVolumeSerde)) // ????????? ??????????????? ????????? serdes??? ??????
				.aggregate(() -> fixedQueue, 
						(k, v, agg) -> agg.add(v), // ????????? add ???????????? ??? ??????????????? ??????	
						(k, v, agg) -> agg.remove(v), // ????????? remove ???????????? ?????? ??????????????? ??????
						Materialized.with(stringSerde, fixedSizePriorityQueueSerde)) // 
				.mapValues(valueMapper)
				.toStream().peek((k, v) -> System.out.println("Stock volume by industry " + k + " " + v))
				.to("stock-volume-by-company", Produced.with(stringSerde, stringSerde)); 
		
		KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
		MockDataProducer.produceStockTransactions(15, 50, 25, false); 
		System.out.println("First Reduction and Aggregation Example Application Started");
		
		kafkaStreams.start();
		
		Thread.sleep(65000);
		
		System.out.println("Shutting down the Reduction and Aggregation Example Application now");
		kafkaStreams.close();
		MockDataProducer.shutdown();		
	}
	
    private static Properties getProperties() {
        
    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KTable-aggregations");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KTable-aggregations-id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KTable-aggregations-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }
}
