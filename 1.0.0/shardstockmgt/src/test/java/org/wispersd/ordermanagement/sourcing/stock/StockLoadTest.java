package org.wispersd.ordermanagement.sourcing.stock;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class) 
@ContextConfiguration(value={"/applicationContextShard.xml"})
public class StockLoadTest {
	@Autowired
	@Qualifier("shardedStockManager")
	private StockManager stockManager;
	
	@Test
	public void testMassGetStocklevel() {
		final int numOfLocs = 6000;
		final int numOfProds = 5000;
		final int selectedProds = 3;
		final int numOfThreads = 1;
		/*
		stockManager.clearAllStockData();
		for(int i=1; i<=numOfProds; i++) {
			AddStockLevelRequest addStockLevelReq = new AddStockLevelRequest();
			for(int j=1; j<=numOfLocs; j++) {
				StockQuantities sq = new StockQuantities();
				sq.setAvailable(500);
				sq.setOversell(0);
				sq.setReserved(0);
				addStockLevelReq.addStockLevel("loc"+j, "prod"+i, sq);
			}
			long start = System.currentTimeMillis();
			stockManager.addStockLevelData(addStockLevelReq);
			long end = System.currentTimeMillis();
			System.out.println("Total time for creating stock level for prod"+i+" is: " + (end-start));
		}
		*/
		final CountDownLatch latch = new CountDownLatch(numOfThreads);
		Runnable r = new Runnable() {
			@Override
			public void run() {
				LocationProducts locProds = new LocationProducts();
				for(int i=1; i<=numOfLocs; i++) {
					for(int j=1; j<=selectedProds; j++) {
						int prodInd = ((int)Math.random()* numOfProds) + 1;
						locProds.addProductForLocation("loc"+i, "prod"+prodInd);
					}
				}
				long start = System.currentTimeMillis();
				stockManager.getAvailableQuantities(locProds);
				long end = System.currentTimeMillis();
				System.out.println("Total time for getting stocklevel is: " + (end-start));
				latch.countDown();
			}
		};
		
		
		for(int t=1; t<=numOfThreads; t++) {
			Thread thread = new Thread(r);
			thread.start();
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
		}
	}
	
	@Test
	public void testMassReserveStocklevel() {
		final int numOfLocs = 6000;
		final int numOfProds = 5000;
		final int numOfThreads = 1;
		final int selectedProds = 3;
		stockManager.clearAllStockData();
		for(int i=1; i<=numOfProds; i++) {
			AddStockLevelRequest addStockLevelReq = new AddStockLevelRequest();
			for(int j=1; j<=numOfLocs; j++) {
				StockQuantities sq = new StockQuantities();
				sq.setAvailable(500);
				sq.setOversell(0);
				sq.setReserved(0);
				addStockLevelReq.addStockLevel("loc"+j, "prod"+i, sq);
			}
			long start = System.currentTimeMillis();
			stockManager.addStockLevelData(addStockLevelReq);
			long end = System.currentTimeMillis();
			System.out.println("Total time for creating stock level for prod"+i+" is: " + (end-start));
		}
		final CountDownLatch latch = new CountDownLatch(numOfThreads);
		Runnable r = new Runnable() {
			@Override
			public void run() {
				ReserveStockLevelRequest stockLevelReq = new ReserveStockLevelRequest();
				for(int i=1; i<=selectedProds; i++) {
					int locInd = ((int)Math.random() * numOfLocs) + 1;
					int prodInd = ((int)Math.random()* numOfProds) + 1;
					stockLevelReq.addStockLevel("loc"+locInd, "prod"+prodInd, 1);
				}
				
				stockManager.reserveAllQuantities(stockLevelReq);
			}
		};	
		
		for(int t=1; t<=numOfThreads; t++) {
			Thread thread = new Thread(r);
			thread.start();
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
		}
	}
	
}
