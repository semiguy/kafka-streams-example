package cbcho.kafka.streams.chapter5;

import java.time.Duration;
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
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;

import cbcho.kafka.streams.chapter5.clients.producer.MockDataProducer;
import cbcho.kafka.streams.chapter5.model.StockTransaction;
import cbcho.kafka.streams.chapter5.model.TransactionSummary;
import cbcho.kafka.streams.chapter5.util.datagen.CustomDateGenerator;
import cbcho.kafka.streams.chapter5.util.datagen.DataGenerator;
import cbcho.kafka.streams.chapter5.util.serde.StreamsSerdes;

import static cbcho.kafka.streams.chapter5.util.Topics.CLIENTS;
import static cbcho.kafka.streams.chapter5.util.Topics.COMPANIES;

public class GlobalKTableExample {
	
	private static final String STOCK_TRANSACTIONS_TOPIC = "stock-transactions";
	
	public static void main(String[] args) throws Exception {
		
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> transactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<TransactionSummary> transactionSummarySerde = StreamsSerdes.TransactionSummarySerde();
        
        StreamsBuilder builder = new StreamsBuilder();
        long twentySeconds = 1000 * 20;
        
        KeyValueMapper<Windowed<TransactionSummary>, Long, KeyValue<String, TransactionSummary>> transactionMapper = (window, count) -> {
        	
        	TransactionSummary transactionSummary = window.key();
        	String newKey = transactionSummary.getIndustry();
        	transactionSummary.setSummaryCount(count);
        	
        	return KeyValue.pair(newKey, transactionSummary);
        };
        
        KStream<String, TransactionSummary> countStream = 
        		builder.stream(STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde, transactionSerde).withOffsetResetPolicy(AutoOffsetReset.LATEST))
        				.groupBy((noKey, transaction) -> TransactionSummary.from(transaction), Serialized.with(transactionSummarySerde, transactionSerde))
        				.windowedBy(SessionWindows.with(twentySeconds)).count()
        				.toStream().map(transactionMapper);
        
        GlobalKTable<String, String> publicCompanies = builder.globalTable(COMPANIES.topicName());
        GlobalKTable<String, String> clients = builder.globalTable(CLIENTS.topicName());
        
        countStream.leftJoin(publicCompanies, (key, txn) -> txn.getStockTicker(), TransactionSummary::withCompanyName)
        		.leftJoin(clients, (key, txn) -> txn.getCustomerId(), TransactionSummary::withCustomerName)
        		.print(Printed.<String, TransactionSummary>toSysOut().withLabel("Resolved Transaction Summaries"));
        
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.cleanUp();
        
        kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
        	
        	System.out.println("had exception " + e);
        });
        
        CustomDateGenerator dateGenerator = CustomDateGenerator.withTimestampsIncreasingBy(Duration.ofMillis(750));
        
        DataGenerator.setTimestampGenerator(dateGenerator::get); 
        
        MockDataProducer.produceStockTransactions(2, 5, 3, true);

        System.out.println("Starting GlobalKTable Example");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        
        Thread.sleep(65000);
        
        System.out.println("Shutting down the GlobalKTable Example Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
		
	}
	
    private static Properties getProperties() {
        
    	Properties props = new Properties();
        
    	props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Global_Ktable_example");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Global_Ktable_example_group_id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Global_Ktable_example_client_id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, StockTransactionTimestampExtractor.class);
        
        return props;

    }
}
