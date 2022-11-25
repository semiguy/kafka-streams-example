package cbcho.kafka.streams.chapter5;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import cbcho.kafka.streams.chapter5.clients.producer.MockDataProducer;
import cbcho.kafka.streams.chapter5.model.StockTickerData;
import cbcho.kafka.streams.chapter5.util.serde.StreamsSerdes;

public class KStreamVsKTableExample {
	
    private static final String STOCK_TICKER_TABLE_TOPIC = "stock-ticker-table";
    private static final String STOCK_TICKER_STREAM_TOPIC = "stock-ticker-stream";
	
	public static void main(String[] args) throws Exception {
		
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());
		
		StreamsBuilder builder = new StreamsBuilder();
		
		KTable<String, StockTickerData> stockTickerTable = builder.table(STOCK_TICKER_TABLE_TOPIC);
		KStream<String, StockTickerData> stockTickerStream = builder.stream(STOCK_TICKER_STREAM_TOPIC);
		
		stockTickerTable.toStream().print(Printed.<String, StockTickerData>toSysOut().withLabel("Stocks-KTable"));
		stockTickerStream.print(Printed.<String, StockTickerData>toSysOut().withLabel("Stocks-KStream"));
		
		int numberCompanies = 3;
        int iterations = 3;
        
        MockDataProducer.produceStockTickerData(numberCompanies, iterations); 
        
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        System.out.println("KTable vs KStream output started");
        
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(15000);
        
        System.out.println("Shutting down KTable vs KStream Application now");
        kafkaStreams.close();
	}
	
    private static Properties getProperties() {
        
    	Properties props = new Properties();
        
    	props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStreamVSKTable_app");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "15000");
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.2.240:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StreamsSerdes.StockTickerSerde().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KStreamVSKTable_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KStreamVSKTable_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        
        return props;

    }
}
