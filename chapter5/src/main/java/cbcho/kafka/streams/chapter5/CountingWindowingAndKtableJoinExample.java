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
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;

import cbcho.kafka.streams.chapter5.clients.producer.MockDataProducer;
import cbcho.kafka.streams.chapter5.model.StockTransaction;
import cbcho.kafka.streams.chapter5.model.TransactionSummary;
import cbcho.kafka.streams.chapter5.util.datagen.CustomDateGenerator;
import cbcho.kafka.streams.chapter5.util.datagen.DataGenerator;
import cbcho.kafka.streams.chapter5.util.serde.StreamsSerdes;

public class CountingWindowingAndKtableJoinExample {
	
	private static final String STOCK_TRANSACTIONS_TOPIC = "stock-transactions";
	
	public static void main(String[] args) throws Exception {
		
		StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> transactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<TransactionSummary> transactionKeySerde = StreamsSerdes.TransactionSummarySerde();

        StreamsBuilder builder = new StreamsBuilder();
        
        long twentySeconds = 1000 * 20;
        long fifteenMinutes = 1000 * 60 * 15;
        long fiveSeconds = 1000 * 5;
		
        KTable<Windowed<TransactionSummary>, Long> customerTransactionCounts = 
        		builder.stream(STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde, transactionSerde).withOffsetResetPolicy(AutoOffsetReset.LATEST))
        		.groupBy((noKey, transaction) -> TransactionSummary.from(transaction), Serialized.with(transactionKeySerde, transactionSerde))
        		// session window comment line below and uncomment another line below for a different window example
        		.windowedBy(SessionWindows.with(twentySeconds).until(fifteenMinutes)).count();
        
        		//The following are examples of different windows examples

		        //Tumbling window with timeout 15 minutes
		        //.windowedBy(TimeWindows.of(twentySeconds).until(fifteenMinutes)).count();
		
		        //Tumbling window with default timeout 24 hours
		        //.windowedBy(TimeWindows.of(twentySeconds)).count();
		
		        //Hopping window 
		        //.windowedBy(TimeWindows.of(twentySeconds).advanceBy(fiveSeconds).until(fifteenMinutes)).count();
        
        customerTransactionCounts.toStream().print(Printed.<Windowed<TransactionSummary>, Long>toSysOut().withLabel("Customer Transactions Counts"));

        KStream<String, TransactionSummary> countStream = customerTransactionCounts.toStream().map((window, count) -> {
                      TransactionSummary transactionSummary = window.key();
                      String newKey = transactionSummary.getIndustry();
                      transactionSummary.setSummaryCount(count);
                      return KeyValue.pair(newKey, transactionSummary);
        });

        KTable<String, String> financialNews = builder.table( "financial-news", Consumed.with(AutoOffsetReset.EARLIEST));


        ValueJoiner<TransactionSummary, String, String> valueJoiner = (txnct, news) ->
                String.format("%d shares purchased %s related news [%s]", txnct.getSummaryCount(), txnct.getStockTicker(), news);

        KStream<String,String> joined = countStream.leftJoin(financialNews, valueJoiner, Joined.with(stringSerde, transactionKeySerde, stringSerde));

        joined.print(Printed.<String, String>toSysOut().withLabel("Transactions and News"));



        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.cleanUp();
        
        kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
            System.out.println("had exception " + e);
        });
        CustomDateGenerator dateGenerator = CustomDateGenerator.withTimestampsIncreasingBy(Duration.ofMillis(750));
        
        DataGenerator.setTimestampGenerator(dateGenerator::get);
        
        MockDataProducer.produceStockTransactions(2, 5, 3, false);

        System.out.println("Starting CountingWindowing and KTableJoins Example");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        
        Thread.sleep(65000);
        
        System.out.println("Shutting down the CountingWindowing and KTableJoins Example Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
        
	}
	
    private static Properties getProperties() {
        
    	Properties props = new Properties();
        
    	props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Windowing-counting-ktable-joins");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Windowing-counting-ktable-joins");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Windowing-counting-ktable-joins");
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
