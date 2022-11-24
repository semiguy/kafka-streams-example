package cbcho.kafka.streams.chapter4.transformer;

import java.util.Objects;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import cbcho.kafka.streams.chapter4.model.Purchase;
import cbcho.kafka.streams.chapter4.model.RewardAccumulator;

public class PurchaseRewardTransformer implements ValueTransformer<Purchase, RewardAccumulator> {
	
	private KeyValueStore<String, Integer> stateStore;
	
	private final String storeName;
	
	private ProcessorContext context;
	
	public PurchaseRewardTransformer(String storeName) {
		
		Objects.requireNonNull(storeName, "Store Name can't be null");
		this.storeName = storeName;
	}

	@Override
	public void init(ProcessorContext context) {
		
		// ProcessorContext에 로컬 참조 설정
		this.context = context;
		// storeName 변수로 StateStore 인스턴스를 찾는다.
		// storeName은 생성자에서 설정한다.
		stateStore = (KeyValueStore) this.context.getStateStore(storeName);
	}

	// 상태를 사용해 Purchase 변환하기
	@Override
	public RewardAccumulator transform(Purchase value) {
		
		// Purchase에서 RewardAccumulator 객체 만들기
		RewardAccumulator rewardAccumulator = RewardAccumulator.builder(value).build();
		
		// 고객 ID로 최신 누적 보상 포인트 가져오기
		Integer accumulatedSoFar = stateStore.get(rewardAccumulator.getCustomerId());
		
		if(accumulatedSoFar != null) {
			// 누적된 숫자가 있으면 현재 합계에 추가한다.
			rewardAccumulator.addRewardPoints(accumulatedSoFar);
		}
		// 새로운 누적 보상 포인트를 stateStore에 저장한다.
		stateStore.put(rewardAccumulator.getCustomerId(), rewardAccumulator.getTotalRewardPoints());
		
		// 새로운 누적 보상 포인트를 반환
		return rewardAccumulator;
	}
	

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
}
