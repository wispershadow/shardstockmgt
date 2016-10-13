package org.wispersd.ordermanagement.sourcing.stock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;


public class StockReqRespTest {
	
	@Test
	public void testStockRequest() {
		Map<String, List<String>> dataMap = new HashMap<String, List<String>>();
		List<String> values1 = new ArrayList<String>();
		values1.add("prod1-3");
		values1.add("prod5-2");
		dataMap.put("loc1", values1);
		
		List<String> values2 = new ArrayList<String>();
		values2.add("prod1-1");
		values2.add("prod2-1");
		dataMap.put("loc3", values2);
		
		List<String> values3 = new ArrayList<String>();
		values3.add("prod2-5");
		dataMap.put("loc4", values2);
		
		
		ReserveStockLevelRequest reserveStockLevelReq1 = new ReserveStockLevelRequest();
		for(String nextLoc: dataMap.keySet()) {
			List<String> values = dataMap.get(nextLoc);
			for(String nextVal: values) {
				String[] arr = nextVal.split("-");
				reserveStockLevelReq1.addStockLevel(nextLoc, arr[0], Integer.valueOf(arr[1]));
			}
		}
		
		
		Map<String, List<String>> resMap = new HashMap<String, List<String>>();
		Iterator<String> allLocs = reserveStockLevelReq1.getAllLocations();
		while(allLocs.hasNext()) {
			String nextLoc = allLocs.next();
			List<String> values = resMap.get(nextLoc);
			if (values == null) {
				values = new ArrayList<String>();
				resMap.put(nextLoc, values);
			}
			Iterator<String> prodsForLoc = reserveStockLevelReq1.getProductsForLocation(nextLoc);
			while(prodsForLoc.hasNext()) {
				String nextProd = prodsForLoc.next();
				Integer stockLevel = reserveStockLevelReq1.getStockLevel(nextLoc, nextProd);
				values.add(nextProd+"-"+stockLevel);
			}
		}
		
		Assert.assertEquals(dataMap.keySet(), resMap.keySet());
		for(String nextLoc: dataMap.keySet()) {
			List<String> nextProds1 = dataMap.get(nextLoc);
			List<String> nextProds2 = resMap.get(nextLoc);
			Assert.assertEquals(nextProds1.size(), nextProds2.size());
			Collections.sort(nextProds1);
			Collections.sort(nextProds2);
			for(int i=0; i<nextProds1.size(); i++) {
				Assert.assertEquals(nextProds1.get(i), nextProds2.get(i));
			}
		}
		
		Iterator<StockRow<Integer>> stockRowIter1 = reserveStockLevelReq1.getStockRowIterator();
		while(stockRowIter1.hasNext()) {
			StockRow<Integer> nextStockRow = stockRowIter1.next();
			Assert.assertTrue(dataMap.containsKey(nextStockRow.getLocationId()));
			Assert.assertTrue(dataMap.get(nextStockRow.getLocationId()).contains(nextStockRow.getProdCode()+"-"+nextStockRow.getValue()));
		}	
	}
	
	@Test
	public void testStockResponse() {
		ReserveStockLevelResponse finalResp = new ReserveStockLevelResponse();
		Iterator<StockRow<Integer>> stockRowIter = finalResp.getStockRowIterator();
		while(stockRowIter.hasNext()) {
			Assert.fail();
		}
		
		ReserveStockLevelResponse sub1 = new ReserveStockLevelResponse();
		sub1.addStockLevel("loc1", "prod1", Integer.valueOf(1));
		sub1.addStockLevel("loc1", "prod3", Integer.valueOf(5));
		sub1.addStockLevel("loc2", "prod4", Integer.valueOf(2));
		finalResp.mergeResponse(sub1);
		
		ReserveStockLevelResponse sub2 = new ReserveStockLevelResponse();
		sub2.addStockLevel("loc2", "prod1", Integer.valueOf(1));
		sub2.addStockLevel("loc2", "prod3", Integer.valueOf(4));
		finalResp.mergeResponse(sub2);
		
		Map<String, String> finalMap = new HashMap<String, String>();
		finalMap.put("loc1", "prod1-1,prod3-5");
		finalMap.put("loc2", "prod4-2,prod1-1,prod3-4");
		stockRowIter = finalResp.getStockRowIterator();
		while(stockRowIter.hasNext()) {
			StockRow<Integer> nextStockRow = stockRowIter.next();
			Assert.assertTrue(finalMap.containsKey(nextStockRow.getLocationId()));
			String allKeys = finalMap.get(nextStockRow.getLocationId());
			Assert.assertTrue(allKeys.indexOf(nextStockRow.getProdCode()+"-"+nextStockRow.getValue()) >= 0);
		}	
	}

}
