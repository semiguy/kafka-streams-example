package cbcho.kafka.streams.chapter4;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import cbcho.kafka.streams.chapter4.model.Purchase;

public class TransactionTimestampExtractor implements TimestampExtractor {

	@Override
	public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
		
		Purchase purchasePurchaseTransaction = (Purchase) record.value();
				
		return purchasePurchaseTransaction.getPurchaseDate().getTime();
	}
	
	
}
