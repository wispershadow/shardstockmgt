package org.wispersd.ordermanagement.sourcing.stock;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Assert;



public class StockTestUtils {
	
	public static  void verifyGetQtyResponse(GetQuantityResponse response, Map<String, List<String>> expectedQty) {
		Iterator<StockRow<Integer>> iter = response.getStockRowIterator();
		while(iter.hasNext()) {
			StockRow<Integer> nextRow = iter.next();
			Assert.assertTrue(expectedQty.containsKey(nextRow.getLocationId()));
			List<String> res = expectedQty.get(nextRow.getLocationId());
			Assert.assertTrue(res.contains(nextRow.getProdCode()+"-"+nextRow.getValue()));
		}
	}
	
	public static void verifyAvailablePositive(GetQuantityResponse response) {
		Iterator<StockRow<Integer>> iter = response.getStockRowIterator();
		while(iter.hasNext()) {
			StockRow<Integer> nextRow = iter.next();
			Assert.assertTrue(nextRow.getValue().intValue() >= 0);
		}	
	}
	
}
