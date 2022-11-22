package cbcho.kafka.streams.chapter2.model;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PurchaseKey {
	
	private String customerId;
	private Date transactionDate;
}
