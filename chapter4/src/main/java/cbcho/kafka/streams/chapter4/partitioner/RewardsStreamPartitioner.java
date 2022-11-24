package cbcho.kafka.streams.chapter4.partitioner;

import org.apache.kafka.streams.processor.StreamPartitioner;

import cbcho.kafka.streams.chapter4.model.Purchase;

public class RewardsStreamPartitioner implements StreamPartitioner<String, Purchase> {

	@Override
	public Integer partition(String topic, String key, Purchase value, int numPartitions) {
		
		// 고객 ID로 파티션을 변경한다.
		return value.getCustomerId().hashCode() % numPartitions;
	}
	
	
}
