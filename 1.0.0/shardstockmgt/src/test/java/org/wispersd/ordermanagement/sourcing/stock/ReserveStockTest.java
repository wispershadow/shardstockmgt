package org.wispersd.ordermanagement.sourcing.stock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class) 
@ContextConfiguration(value={"/applicationContextShard.xml"})
public class ReserveStockTest {
	@Autowired
	@Qualifier("shardedStockManager")
	private StockManager stockManager;

	@Test
	public void testSuccessReserveAllStockLevel() {
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
				sq.setOversell(5);
				sq.setReserved(10);
				availableList.add("prod"+j + "-0");
				instockList.add("prod"+j + "-100");
				reservedList.add("prod"+j + "-105");
				addStockLevelReq.addStockLevel("loc"+i, "prod"+j, sq);
			}
		}
		
		stockManager.addStockLevelData(addStockLevelReq);
		
		ReserveStockLevelRequest reserveReq = new ReserveStockLevelRequest();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				reserveReq.addStockLevel("loc"+i, "prod"+j, 95);
			}
		}
		boolean reserveRes = stockManager.reserveAllQuantities(reserveReq);
		Assert.assertTrue(reserveRes);
		
		LocationProducts locProds = new LocationProducts();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				locProds.addProductForLocation("loc"+i, "prod"+j);
			}
		}	
		GetQuantityResponse respInstock = stockManager.getInstockQuantities(locProds);;
		GetQuantityResponse respReserved = stockManager.getReservedQuantities(locProds);
		GetQuantityResponse respAvailable = stockManager.getAvailableQuantities(locProds);
		StockTestUtils.verifyGetQtyResponse(respAvailable, expectedAvailable);
		StockTestUtils.verifyGetQtyResponse(respInstock, expectedInstock);
		StockTestUtils.verifyGetQtyResponse(respReserved, expectedReserved);
	}
	
	
	
	
	@Test
	public void testFailReserveAllStockLevel() {
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
				sq.setOversell(5);
				sq.setReserved(10);
				availableList.add("prod"+j + "-95");
				instockList.add("prod"+j + "-100");
				reservedList.add("prod"+j + "-10");
				addStockLevelReq.addStockLevel("loc"+i, "prod"+j, sq);
			}
		}
		
		stockManager.addStockLevelData(addStockLevelReq);
		ReserveStockLevelRequest reserveReq = new ReserveStockLevelRequest();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				if (i % 2 == 0) {
					reserveReq.addStockLevel("loc"+i, "prod"+j, 15);
				}
				else {
					reserveReq.addStockLevel("loc"+i, "prod"+j, 100);
				}
			}
		}
		boolean reserveRes = stockManager.reserveAllQuantities(reserveReq);
		Assert.assertFalse(reserveRes);
		
		
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
	
	
	
	
	
	@Test
	public void testSuccessReservePartialStockLevel() {
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
				sq.setOversell(5);
				sq.setReserved(10);
				availableList.add("prod"+j + "-0");
				instockList.add("prod"+j + "-100");
				reservedList.add("prod"+j + "-105");
				addStockLevelReq.addStockLevel("loc"+i, "prod"+j, sq);
			}
		}
		
		stockManager.addStockLevelData(addStockLevelReq);
		ReserveStockLevelRequest reserveReq = new ReserveStockLevelRequest();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				reserveReq.addStockLevel("loc"+i, "prod"+j, 95);
			}
		}
		ReserveStockLevelResponse response = stockManager.reservePartialQuantities(reserveReq);
		Iterator<StockRow<Integer>> rowIter = response.getStockRowIterator();
		if (rowIter.hasNext()) {
			Assert.fail();
		}
		
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
	
	@Test
	public void testRandomReserveAll() {
		int numOfLocs = 10;
		int numOfProds = 5;
		stockManager.clearAllStockData();
		AddStockLevelRequest addStockLevelReq = new AddStockLevelRequest();
		Map<String, List<String>> expectedInstock = new HashMap<String, List<String>>();
		for(int i=1; i<=numOfLocs; i++) {
			List<String> instockList = new ArrayList<String>();
			expectedInstock.put("loc"+i, instockList);
			for(int j=1; j<=numOfProds; j++) {
				StockQuantities sq = new StockQuantities();
				int availableQty = 120;
				sq.setAvailable(availableQty);
				sq.setOversell(5);
				sq.setReserved(0);
				instockList.add("prod"+j + "-"+availableQty);
				addStockLevelReq.addStockLevel("loc"+i, "prod"+j, sq);
			}
		}
		stockManager.addStockLevelData(addStockLevelReq);
		
		int numConcurrent = 5;
		CountDownLatch latch = new CountDownLatch(numConcurrent);
		ConcurrentLinkedQueue<String> reservedQtys = new ConcurrentLinkedQueue<String>();
		for(int i=0; i<numConcurrent; i++) {
			RandomReserveAllRunnable r = new RandomReserveAllRunnable(reservedQtys, stockManager, latch);
			Thread t = new Thread(r);
			t.start();
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
		}
		
		LocationProducts locProds = new LocationProducts();
		for(int i=1; i<=numOfLocs; i++) {
			for(int j=1; j<=numOfProds; j++) {
				locProds.addProductForLocation("loc"+i, "prod"+j);
			}
		}	
		Map<String, List<String>> totalReserved = getReserveQtyMap(reservedQtys);
		GetQuantityResponse respInstock = stockManager.getInstockQuantities(locProds);
		GetQuantityResponse respReserved = stockManager.getReservedQuantities(locProds);
		GetQuantityResponse respAvailable = stockManager.getAvailableQuantities(locProds);
		StockTestUtils.verifyGetQtyResponse(respInstock, expectedInstock);
		StockTestUtils.verifyGetQtyResponse(respReserved, totalReserved);
		StockTestUtils.verifyAvailablePositive(respAvailable);
	}
	
	
	@Test
	public void testRandomReservePartial() {
		int numOfLocs = 10;
		int numOfProds = 5;
		stockManager.clearAllStockData();
		AddStockLevelRequest addStockLevelReq = new AddStockLevelRequest();
		Map<String, List<String>> expectedInstock = new HashMap<String, List<String>>();
		for(int i=1; i<=numOfLocs; i++) {
			List<String> instockList = new ArrayList<String>();
			expectedInstock.put("loc"+i, instockList);
			for(int j=1; j<=numOfProds; j++) {
				StockQuantities sq = new StockQuantities();
				int availableQty = 120;
				sq.setAvailable(availableQty);
				sq.setOversell(5);
				sq.setReserved(0);
				instockList.add("prod"+j + "-"+availableQty);
				addStockLevelReq.addStockLevel("loc"+i, "prod"+j, sq);
			}
		}
		stockManager.addStockLevelData(addStockLevelReq);
		
		int numConcurrent = 5;
		CountDownLatch latch = new CountDownLatch(numConcurrent);
		ConcurrentLinkedQueue<String> reservedQtys = new ConcurrentLinkedQueue<String>();
		for(int i=0; i<numConcurrent; i++) {
			RandomReservePartialRunnable r = new RandomReservePartialRunnable(reservedQtys, stockManager, latch);
			Thread t = new Thread(r);
			t.start();
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
		}
		
		LocationProducts locProds = new LocationProducts();
		for(int i=1; i<=numOfLocs; i++) {
			for(int j=1; j<=numOfProds; j++) {
				locProds.addProductForLocation("loc"+i, "prod"+j);
			}
		}	
		Map<String, List<String>> totalReserved = getReserveQtyMap(reservedQtys);
		GetQuantityResponse respInstock = stockManager.getInstockQuantities(locProds);
		GetQuantityResponse respReserved = stockManager.getReservedQuantities(locProds);
		GetQuantityResponse respAvailable = stockManager.getAvailableQuantities(locProds);
		StockTestUtils.verifyGetQtyResponse(respInstock, expectedInstock);
		StockTestUtils.verifyGetQtyResponse(respReserved, totalReserved);
		StockTestUtils.verifyAvailablePositive(respAvailable);
	}
	
	
	@Test
	public void testCommitAllStockLevel() {
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
				sq.setOversell(5);
				sq.setReserved(10);
				availableList.add("prod"+j + "-65");
				instockList.add("prod"+j + "-70");
				reservedList.add("prod"+j + "-10");
				addStockLevelReq.addStockLevel("loc"+i, "prod"+j, sq);
			}
		}
		
		stockManager.addStockLevelData(addStockLevelReq);
		ReserveStockLevelRequest reserveReq = new ReserveStockLevelRequest();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				reserveReq.addStockLevel("loc"+i, "prod"+j, 30);
			}
		}
		stockManager.reserveAllQuantities(reserveReq);
		
		CommitStockLevelRequest commitReq1 = new CommitStockLevelRequest();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				commitReq1.addStockLevel("loc"+i, "prod"+j, 15);
			}
		}
		stockManager.commitAllQuantities(commitReq1);
		
		CommitStockLevelRequest commitReq2 = new CommitStockLevelRequest();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				commitReq2.addStockLevel("loc"+i, "prod"+j, 15);
			}
		}
		stockManager.commitAllQuantities(commitReq2);

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
	
	@Test
	public void testCommitPartialStockLevel() {
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
				sq.setAvailable(35);
				sq.setOversell(5);
				sq.setReserved(40);
				if (i % 2 == 0) {
					availableList.add("prod"+j + "-0");
					instockList.add("prod"+j + "-0");
					reservedList.add("prod"+j + "-5");
				}
				else {
					availableList.add("prod"+j + "-0");
					instockList.add("prod"+j + "-15");
					reservedList.add("prod"+j + "-20");
				}
				addStockLevelReq.addStockLevel("loc"+i, "prod"+j, sq);
			}
		}
		stockManager.addStockLevelData(addStockLevelReq);
		
		CommitStockLevelRequest commitReq = new CommitStockLevelRequest();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				if (i % 2 == 0) {
					commitReq.addStockLevel("loc"+i, "prod"+j, 40);
				}
				else {
					commitReq.addStockLevel("loc"+i, "prod"+j, 20);
				}
			}
		}
		commitReq.addStockLevel("loc1", "prod0", Integer.valueOf(23));
		commitReq.addStockLevel("loc0", "prod8", Integer.valueOf(15));
		
		CommitStockLevelResponse commitResp = stockManager.commitPartitalQuantities(commitReq);
		Map<String, Map<String, Integer>> respMap = commitResp.toMap();
		Assert.assertTrue(respMap.containsKey("loc0"));
		Assert.assertTrue(respMap.get("loc1").containsKey("prod0")); 
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
	
	
	@Test
	public void testRollbackStockLevel() {
		stockManager.clearAllStockData();
		AddStockLevelRequest addStockLevelReq = new AddStockLevelRequest();
		Map<String, List<String>> expectedInstock = new HashMap<String, List<String>>();
		Map<String, List<String>> expectedReserved = new HashMap<String, List<String>>();
		for(int i=1; i<=10; i++) {
			List<String> instockList = new ArrayList<String>();
			expectedInstock.put("loc"+i, instockList);
			for(int j=1; j<=5; j++) {
				StockQuantities sq = new StockQuantities();
				sq.setAvailable(100);
				sq.setOversell(0);
				sq.setReserved(0);
				instockList.add("prod"+j + "-100");
				addStockLevelReq.addStockLevel("loc"+i, "prod"+j, sq);
			}
		}
		
		stockManager.addStockLevelData(addStockLevelReq);
		
		ReserveStockLevelRequest reserveReq = new ReserveStockLevelRequest();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				reserveReq.addStockLevel("loc"+i, "prod"+j, 80);
			}
		}
		stockManager.reserveAllQuantities(reserveReq);
		
		RollbackStockLevelRequest rollbackReq = new RollbackStockLevelRequest();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				int rollbackQty = (int)Math.random()*80;
				rollbackReq.addStockLevel("loc"+i, "prod"+j, Integer.valueOf(rollbackQty));
			}
		}
		stockManager.rollbackQuantities(rollbackReq);
		Iterator<StockRow<Integer>> iter = rollbackReq.getStockRowIterator();
		while(iter.hasNext()) {
			StockRow<Integer> nextRow = iter.next();
			String nextLoc = nextRow.getLocationId();
			List<String> prodQty = expectedReserved.get(nextLoc);
			if (prodQty == null) {
				prodQty = new ArrayList<String>();
				expectedReserved.put(nextLoc, prodQty);
			}
			prodQty.add(nextRow.getProdCode()+"-"+(80-nextRow.getValue()));
		}
		LocationProducts locProds = new LocationProducts();
		for(int i=1; i<=10; i++) {
			for(int j=1; j<=5; j++) {
				locProds.addProductForLocation("loc"+i, "prod"+j);
			}
		}	
		GetQuantityResponse respInstock = stockManager.getInstockQuantities(locProds);
		GetQuantityResponse respReserved = stockManager.getReservedQuantities(locProds);
		StockTestUtils.verifyGetQtyResponse(respInstock, expectedInstock);
		StockTestUtils.verifyGetQtyResponse(respReserved, expectedReserved);
	}
	
	
	private static Map<String, List<String>> getReserveQtyMap(ConcurrentLinkedQueue<String> reservedQtys) {
		Map<String, Map<String, AtomicInteger>> tmp = new HashMap<String, Map<String, AtomicInteger>>();
		for(String nextQty: reservedQtys) {
			String[] arr = nextQty.split("-");
			String loc = arr[0];
			String prod = arr[1];
			int qty = Integer.valueOf(arr[2]);
			Map<String, AtomicInteger> prodQty = tmp.get(loc);
			if (prodQty == null) {
				prodQty = new HashMap<String, AtomicInteger>();
				tmp.put(loc, prodQty);
			}
			AtomicInteger totalRsv = prodQty.get(prod);
			if (totalRsv == null) {
				totalRsv = new AtomicInteger(0);
				prodQty.put(prod, totalRsv);
			}
			totalRsv.addAndGet(qty);
		}
		
		Map<String, List<String>> res = new HashMap<String, List<String>>();
		for(String nextLoc: tmp.keySet()) {
			List<String> lst = new ArrayList<String>();
			res.put(nextLoc, lst);
			Map<String, AtomicInteger> prodQty = tmp.get(nextLoc);
			for(String nextProd: prodQty.keySet()) {
				AtomicInteger nextQty = prodQty.get(nextProd);
				lst.add(nextProd+"-"+nextQty.intValue());
			}
		}
		return res;
	}
	
	
	static class RandomReserveAllRunnable implements Runnable {
		private final ConcurrentLinkedQueue<String> reservedQtys;
		private final StockManager stockManager;
		private final CountDownLatch latch;
		
		public RandomReserveAllRunnable(ConcurrentLinkedQueue<String> reservedQtys, StockManager stockManager,  CountDownLatch latch) {
			this.reservedQtys = reservedQtys;
			this.stockManager = stockManager;
			this.latch = latch;
		}
		
		@Override
		public void run() {
			ReserveStockLevelRequest reserveReq = new ReserveStockLevelRequest();
			for(int i=1; i<=10; i++) {
				for(int j=1; j<=5; j++) {
					int qtyToReserve = (int)(Math.random() * 50);
					reserveReq.addStockLevel("loc"+i, "prod"+j, Integer.valueOf(qtyToReserve));
				}
			}
			boolean res = stockManager.reserveAllQuantities(reserveReq);
			Iterator<StockRow<Integer>> iter = reserveReq.getStockRowIterator();
			while(iter.hasNext()) {
				StockRow<Integer> nextRow = iter.next();
				if (res) { 
					reservedQtys.add(nextRow.getLocationId()+"-"+nextRow.getProdCode()+"-"+nextRow.getValue());
				}
				else {
					reservedQtys.add(nextRow.getLocationId()+"-"+nextRow.getProdCode()+"-0");
				}
			}
			latch.countDown();
		}
	}	
	
	static class RandomReservePartialRunnable implements Runnable {
		private final ConcurrentLinkedQueue<String> reservedQtys;
		private final StockManager stockManager;
		private final CountDownLatch latch;
		
		public RandomReservePartialRunnable(ConcurrentLinkedQueue<String> reservedQtys, StockManager stockManager,  CountDownLatch latch) {
			this.reservedQtys = reservedQtys;
			this.stockManager = stockManager;
			this.latch = latch;
		}


		@Override
		public void run() {
			ReserveStockLevelRequest reserveReq = new ReserveStockLevelRequest();
			for(int i=1; i<=10; i++) {
				for(int j=1; j<=5; j++) {
					int qtyToReserve = (int)(Math.random() * 50);
					reserveReq.addStockLevel("loc"+i, "prod"+j, Integer.valueOf(qtyToReserve));
				}
			}
			ReserveStockLevelResponse reserveResp = stockManager.reservePartialQuantities(reserveReq);
			Iterator<StockRow<Integer>> iter = reserveReq.getStockRowIterator();
			while(iter.hasNext()) {
				StockRow<Integer> nextRow = iter.next();
				Integer remain = reserveResp.getStockLevel(nextRow.getLocationId(), nextRow.getProdCode());
				if (remain == null) { 
					reservedQtys.add(nextRow.getLocationId()+"-"+nextRow.getProdCode()+"-"+nextRow.getValue());
				}
				else {
					int curQty = nextRow.getValue() - remain.intValue();
					reservedQtys.add(nextRow.getLocationId()+"-"+nextRow.getProdCode()+"-"+curQty);
				}
			}
			latch.countDown();
		}
		
	}
}
