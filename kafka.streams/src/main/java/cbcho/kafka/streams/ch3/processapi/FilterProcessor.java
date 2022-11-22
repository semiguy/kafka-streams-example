package cbcho.kafka.streams.ch3.processapi;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

// 스트림 프로세서 클래스를 생성하기 위해서는 kafka-streams 라이브러리에서 제공하는
// Processor 또는 Transformer 인터페이스를 사용해야 한다.
public class FilterProcessor implements Processor<String, String> {
	
	// ProcessorContext 클래스는 프로세서에 대한 정보를 담고 있다.
	private ProcessorContext context;
	
	// 스트림 프로세서의 생성자
	@Override
	public void init(ProcessorContext context) {
		
		this.context = context;
	}
	
	// 프로세싱 로직
	@Override
	public void process(String key, String value) {
		
		if(value.length() > 5) {
			// 필터링된 데이터의 경우 forward() 메서드를 사용하여 
			// 다음 토폴로지(다음 프로세서)로 넘어가도록 한다.
			context.forward(key, value); 
		}
		// 처리가 완료된 이후에는 commit()을 호출하여 명시적으로 데이터가 처리되었음을 선언
		context.commit();
		
	}
	
	// FilterProcessor가 종료되기 전에 호출되는 메서드다.
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	
}
