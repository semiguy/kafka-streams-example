package cbcho.kafka.streams.chapter2.partitioner;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

import cbcho.kafka.streams.chapter2.model.PurchaseKey;

public class PurchaseKeyPartitioner extends DefaultPartitioner {

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		
		Object newKey = null;
		
		if(key != null) {
			
			PurchaseKey purchaseKey = (PurchaseKey) key;
			newKey = purchaseKey.getCustomerId();
			keyBytes = ((String) newKey).getBytes();
		}
		
		return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
	}
}
