package org.wispersd.ordermanagement.sourcing.stock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class) 
@ContextConfiguration(value={"/applicationContextShard.xml"})
public class StockLevelTest {
	@Autowired
	@Qualifier("shardedStockManager")
	private StockManager stockManager;
	
	@Test
	public void testAddStockLevel() {
		stockManager.clearAllStockData();
		AddStockLevelRequest addStockLevelReq = new AddStockLevelRequest();
		Map<String, List<String>> expectedAvailable = new HashMap<String, List<String>>();
		Map<String, List<String>> expectedInstock = new HashMap<String, List<String>>();
		Map<String, List<String>> expectedReserved = new HashMap<String, List<String>>();
		for(int i=1; i<=10; i++) {
			List<String> availableList = new ArrayList<String>();
			List<String> instockList = new ArrayList<String>();
			List<String> reservedList = new ArrayList<String>();
			expectedAvailable.put("loc"+i, availableList);
			expectedInstock.put("loc"+i, instockList);
			expectedReserved.put("loc"+i, reservedList);
			for(int j=1; j<=5; j++) {
				StockQuantities sq = new StockQuantities();
				sq.setAvailable(100);
				sq.setOversell(0);
				sq.setReserved(0);
				availableList.add("prod"+j + "-100");
				instockList.add("prod"+j + "-100");
				reservedList.add("prod"+j + "-0");
				addStockLevelReq.addStockLevel("loc"+i, "prod"+j, sq);
			}
		}
		
		stockManager.addStockLevelData(addStockLevelReq);
		
		LocationProducts locProds = new LocationProducts();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				locProds.addProductForLocation("loc"+i, "prod"+j);
			}
		}	
		GetQuantityResponse respInstock = stockManager.getInstockQuantities(locProds);
		GetQuantityResponse respReserved = stockManager.getReservedQuantities(locProds);
		GetQuantityResponse respAvailable = stockManager.getAvailableQuantities(locProds);
		StockTestUtils.verifyGetQtyResponse(respAvailable, expectedAvailable);
		StockTestUtils.verifyGetQtyResponse(respInstock, expectedInstock);
		StockTestUtils.verifyGetQtyResponse(respReserved, expectedReserved);
	}
}
