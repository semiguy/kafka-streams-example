package cbcho.kafka.streams.chapter4.joiner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.kafka.streams.kstream.ValueJoiner;

import cbcho.kafka.streams.chapter4.model.CorrelatedPurchase;
import cbcho.kafka.streams.chapter4.model.Purchase;

public class PurchaseJoiner implements ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {

	@Override
	public CorrelatedPurchase apply(Purchase purchase, Purchase otherPurchase) {
		
		// 빌더를 생성
		CorrelatedPurchase.Builder builder = CorrelatedPurchase.newBuilder();
		
		Date purchaseDate = purchase != null ? purchase.getPurchaseDate() : null;
		Double price = purchase != null ? purchase.getPrice() : 0.0;
		// 외부(outer)조인의 경우 null Purchase를 다룬다.
		String itemPurchased = purchase != null ? purchase.getItemPurchased() : null;
		
		Date otherPurchaseDate = otherPurchase != null ? otherPurchase.getPurchaseDate() : null;
		Double otherPrice = otherPurchase != null ? otherPurchase.getPrice() : 0.0;
		// 왼쪽 외부(left outer)조인의 경우 null Purchase를 다룬다
		String otherItemPurchased = otherPurchase != null ? otherPurchase.getItemPurchased() : null;
		
		List<String> purchasedItems = new ArrayList<String>();
		
		if(itemPurchased != null) {
			purchasedItems.add(itemPurchased);
		}
		
		if(otherItemPurchased != null) {
			purchasedItems.add(otherItemPurchased);
		}
		
		String customerId = purchase != null ? purchase.getCustomerId() : null;
		String otherCustomerId = otherPurchase != null ? otherPurchase.getCustomerId() : null;
		
		builder.withCustomerId(customerId != null ? customerId : otherCustomerId)
				.withFirstPurchaseDate(purchaseDate)
				.withSecondPurchaseDate(otherPurchaseDate)
				.withItemsPurchased(purchasedItems)
				.withTotalAmount(price + otherPrice);
		
		// 새로운 CorrelatedPurchase 객체를 반환 
		return builder.build();
	}
	
	
}
