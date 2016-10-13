/*
 * [y] hybris Platform
 *
 * Copyright (c) 2000-2016 SAP SE
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * Hybris ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the
 * terms of the license agreement you entered into with SAP Hybris.
 */
package org.wispersd.ordermanagement.sourcing.stock.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wispersd.core.data.nosql.redis.JedisDataManager;
import org.wispersd.core.data.nosql.redis.JedisUtils;
import org.wispersd.ordermanagement.sourcing.stock.AbstractStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.AddStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.CommitStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.CommitStockLevelResponse;
import org.wispersd.ordermanagement.sourcing.stock.GetQuantityResponse;
import org.wispersd.ordermanagement.sourcing.stock.LocationProducts;
import org.wispersd.ordermanagement.sourcing.stock.ReserveStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.ReserveStockLevelResponse;
import org.wispersd.ordermanagement.sourcing.stock.RollbackStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.ShardStockUtils;
import org.wispersd.ordermanagement.sourcing.stock.StockManager;
import org.wispersd.ordermanagement.sourcing.stock.StockQuantities;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ShardedJedis;


/**
 *
 */
public class ShardedRedisStockManagerImpl implements StockManager
{
	private static final Logger logger = LoggerFactory.getLogger(ShardedRedisStockManagerImpl.class);
	private JedisDataManager jedisDataManager;
	private static final String TABLENAME = "stock";
	private RedisStockOperationTemplate redisStockOperationTemplate;
	private ExecutorService executorService;

	/**
	 * @return the jedisDataManager
	 */
	public JedisDataManager getJedisDataManager()
	{
		return jedisDataManager;
	}

	/**
	 * @param jedisDataManager
	 *           the jedisDataManager to set
	 */
	public void setJedisDataManager(final JedisDataManager jedisDataManager)
	{
		this.jedisDataManager = jedisDataManager;
	}


	/**
	 * @return the redisStockOperationTemplate
	 */
	public RedisStockOperationTemplate getRedisStockOperationTemplate()
	{
		return redisStockOperationTemplate;
	}

	/**
	 * @param redisStockOperationTemplate
	 *           the redisStockOperationTemplate to set
	 */
	public void setRedisStockOperationTemplate(final RedisStockOperationTemplate redisStockOperationTemplate)
	{
		this.redisStockOperationTemplate = redisStockOperationTemplate;
	}
	
	
	public ExecutorService getExecutorService() {
		return executorService;
	}

	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

	@Override
	public void clearAllStockData()
	{
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final Collection<Jedis> allShards = ((ShardedJedis) jedis).getAllShards();
				for (final Jedis nextShard : allShards)
				{
					nextShard.flushDB();
				}
			}

		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}

	}

	@Override
	public void addStockLevelData(final AddStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Add Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final Map<Jedis, ? extends AbstractStockLevelRequest<StockQuantities>> splitRes = ShardStockUtils
						.splitRequestByShard(stockLevelReq, (ShardedJedis) jedis);
				final CountDownLatch countdownLatch = new CountDownLatch(splitRes.size());
				for (final Jedis nextShard : splitRes.keySet())
				{
					final Runnable r = new Runnable()
					{

						@Override
						public void run()
						{
							try {
								final AddStockLevelRequest reqPerShard = (AddStockLevelRequest) splitRes.get(nextShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Add Stock level request for shard: " + nextShard + " is: " + reqPerShard);
								}
								redisStockOperationTemplate.addStockLevelData(nextShard, reqPerShard);
							}
							catch (final Exception e)
							{
								logger.error("Error trying to add stock level: ", e);
							}
							countdownLatch.countDown();
						}
					};
					executorService.submit(r);
				}
				try
				{
					countdownLatch.await();
				}
				catch (final InterruptedException e)
				{
					logger.warn("Thread execution interruptted");
				}
			}
			else
			{
				throw new RuntimeException("Not a sharded jedis");
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	@Override
	public boolean reserveAllQuantities(final ReserveStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Reserve Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final Map<Jedis, ? extends AbstractStockLevelRequest<Integer>> splitRes = ShardStockUtils.splitRequestByShard(
						stockLevelReq, (ShardedJedis) jedis);
				final CountDownLatch countdownLatch1 = new CountDownLatch(splitRes.size());
				final Object[][] tempRes = new Object[splitRes.size()][3];
				final AtomicInteger countSuccess = new AtomicInteger(0);
				final AtomicBoolean toRollback = new AtomicBoolean(false);
				int ind = 0;
				for (final Jedis nextShard : splitRes.keySet())
				{
					final int i = ind;
					final Runnable r = new Runnable()
					{
						@Override
						public void run()
						{
							final ReserveStockLevelRequest reqPerShard = (ReserveStockLevelRequest) splitRes.get(nextShard);
							if (logger.isDebugEnabled())
							{
								logger.debug("Reserve Stock level request for shard: " + nextShard + " is: " + reqPerShard);
							}
							try {
								
								final boolean opRes = redisStockOperationTemplate.reserveAllQuantities(nextShard, reqPerShard);
								if (!opRes)
								{
									toRollback.set(true);
								}
								else
								{
									countSuccess.incrementAndGet();
								}
								tempRes[i][0] = nextShard;
								tempRes[i][1] = reqPerShard;
								tempRes[i][2] = Boolean.valueOf(opRes);
							}
							catch (final Exception e)
							{
								logger.error("Error trying to reserve quantity: ", e);
								toRollback.set(true);
								tempRes[i][0] = nextShard;
								tempRes[i][1] = reqPerShard;
								tempRes[i][2] = Boolean.valueOf(false);
							}
							countdownLatch1.countDown();
						}
					};
					executorService.submit(r);
					ind++;
				}
				try
				{
					countdownLatch1.await();
				}
				catch (final InterruptedException e)
				{
					logger.warn("Thread execution interruptted");
				}
				if (toRollback.get())
				{
					logger.warn("Some reservation failed, perform roll back");
					final CountDownLatch countdownLatch2 = new CountDownLatch(countSuccess.get());
					for (final Object[] nextPair : tempRes)
					{
						final Jedis nextShard = (Jedis) nextPair[0];
						final ReserveStockLevelRequest reqPerShard = (ReserveStockLevelRequest) nextPair[1];
						final boolean b = ((Boolean) nextPair[2]).booleanValue();
						if (b)
						{
							final Runnable r = new Runnable()
							{

								@Override
								public void run()
								{
									if (logger.isDebugEnabled())
									{
										logger.debug("Rollback request for shard: " + nextShard + " is: " + reqPerShard);
									}
									try
									{
										redisStockOperationTemplate.rollbackQuantities(nextShard, reqPerShard);
									}
									catch (final Exception e)
									{
										logger.error("Error trying to rollback quantity", e);
									}
									countdownLatch2.countDown();
								}
							};
							executorService.submit(r);
						}
					}
					try
					{
						countdownLatch2.await();
					}
					catch (final InterruptedException e)
					{
						logger.warn("Thread execution interruptted");
					}
				}
				else {
					return true;
				}
			}
			else
			{
				throw new RuntimeException("Not a sharded jedis");
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
		return false;
	}

	@Override
	public ReserveStockLevelResponse reservePartialQuantities(final ReserveStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Reserve Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final ReserveStockLevelResponse finalResp = new ReserveStockLevelResponse();
				final Map<Jedis, ? extends AbstractStockLevelRequest<Integer>> splitRes = ShardStockUtils.splitRequestByShard(
						stockLevelReq, (ShardedJedis) jedis);
				final CountDownLatch countdownLatch = new CountDownLatch(splitRes.size());
				for (final Jedis nextShard : splitRes.keySet())
				{
					final Runnable r = new Runnable()
					{
						@Override
						public void run()
						{
							try {
								final ReserveStockLevelRequest reqPerShard = (ReserveStockLevelRequest) splitRes.get(nextShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial Reserve Stock level request for shard: " + nextShard + " is: " + reqPerShard);
								}
								final ReserveStockLevelResponse shardResp = redisStockOperationTemplate.reservePartialQuantities(nextShard,
										reqPerShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial Reserve Stock level response for shard: " + nextShard + " is: " + shardResp);
								}
								finalResp.mergeResponse(shardResp);
							}
							catch (final Exception e)
							{
								logger.error("Error trying to reserve quantity", e);
							}
							countdownLatch.countDown();
						}
					};
					executorService.submit(r);
				}
				try
				{
					countdownLatch.await();
				}
				catch (final InterruptedException e)
				{
					logger.warn("Thread execution interruptted");
				}
				return finalResp;
			}
			else
			{
				throw new RuntimeException("Not a sharded jedis");
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	@Override
	public boolean commitAllQuantities(final CommitStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Commit Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final Map<Jedis, ? extends AbstractStockLevelRequest<Integer>> splitRes = ShardStockUtils.splitRequestByShard(
						stockLevelReq, (ShardedJedis) jedis);
				final CountDownLatch countdownLatch = new CountDownLatch(splitRes.size());
				for (final Jedis nextShard : splitRes.keySet())
				{
					final Runnable r = new Runnable()
					{
						@Override
						public void run()
						{
							try {
								final CommitStockLevelRequest reqPerShard = (CommitStockLevelRequest) splitRes.get(nextShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial Commit Stock level request for shard: " + nextShard + " is: " + reqPerShard);
								}
								redisStockOperationTemplate.commitAllQuantities(nextShard, reqPerShard);
							}
							catch (final Exception e)
							{
								logger.error("Error trying to commit all quantity", e);
							}
							countdownLatch.countDown();
						}
					};
					executorService.submit(r);
				}
				try
				{
					countdownLatch.await();
				}
				catch (final InterruptedException e)
				{
					logger.warn("Thread execution interruptted");
				}
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
		return false;
	}

	@Override
	public CommitStockLevelResponse commitPartitalQuantities(final CommitStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Commit Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final CommitStockLevelResponse finalResp = new CommitStockLevelResponse();
				final Map<Jedis, ? extends AbstractStockLevelRequest<Integer>> splitRes = ShardStockUtils.splitRequestByShard(
						stockLevelReq, (ShardedJedis) jedis);
				final CountDownLatch countdownLatch = new CountDownLatch(splitRes.size());
				for (final Jedis nextShard : splitRes.keySet())
				{
					final Runnable r = new Runnable()
					{
						@Override
						public void run()
						{
							try {
								final CommitStockLevelRequest reqPerShard = (CommitStockLevelRequest) splitRes.get(nextShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial Reserve Stock level request for shard: " + nextShard + " is: " + reqPerShard);
								}
								final CommitStockLevelResponse shardResp = redisStockOperationTemplate.commitPartialQuantities(nextShard,
										reqPerShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial Reserve Stock level response for shard: " + nextShard + " is: " + shardResp);
								}
								finalResp.mergeResponse(shardResp);
							}
							catch (final Exception e)
							{
								logger.error("Error trying to commit partial quantity", e);
							}
							countdownLatch.countDown();
						}
					};
					executorService.submit(r);
				}
				try
				{
					countdownLatch.await();
				}
				catch (final InterruptedException e)
				{
					logger.warn("Thread execution interruptted");
				}
				return finalResp;
			}
			else
			{
				throw new RuntimeException("Not a sharded jedis");
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	@Override
	public void rollbackQuantities(final RollbackStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Rollback Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final Map<Jedis, ? extends AbstractStockLevelRequest<Integer>> splitRes = ShardStockUtils.splitRequestByShard(
						stockLevelReq, (ShardedJedis) jedis);
				final CountDownLatch countdownLatch = new CountDownLatch(splitRes.size());
				for (final Jedis nextShard : splitRes.keySet())
				{
					final Runnable r = new Runnable()
					{
						@Override
						public void run()
						{
							try {
								final RollbackStockLevelRequest reqPerShard = (RollbackStockLevelRequest) splitRes.get(nextShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial Rollback Stock level request for shard: " + nextShard + " is: " + reqPerShard);
								}
								redisStockOperationTemplate.rollbackQuantities(nextShard, reqPerShard);
							}
							catch (final Exception e)
							{
								logger.error("Error trying to rollback quantity", e);
							}
							countdownLatch.countDown();
						}
					};
					executorService.submit(r);
				}
				try
				{
					countdownLatch.await();
				}
				catch (final InterruptedException e)
				{
					logger.warn("Thread execution interruptted");
				}
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	@Override
	public GetQuantityResponse getAvailableQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get Available Quantity Request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final GetQuantityResponse finalResp = new GetQuantityResponse();
				final Map<Jedis, LocationProducts> splitRes = ShardStockUtils.splitLocProdByShard(locProds, (ShardedJedis) jedis);
				final CountDownLatch countdownLatch = new CountDownLatch(splitRes.size());
				for (final Jedis nextShard : splitRes.keySet())
				{
					final Runnable r = new Runnable()
					{
						@Override
						public void run()
						{
							try {
								final LocationProducts reqPerShard = splitRes.get(nextShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial get quantity request for shard: " + nextShard + " is: " + reqPerShard);
								}
								final GetQuantityResponse shardResp = redisStockOperationTemplate.getAvailableQuantities(nextShard,
										reqPerShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial get quantity response for shard: " + nextShard + " is: " + shardResp);
								}
								finalResp.mergeResponse(shardResp);
							}
							catch (final Exception e)
							{
								logger.error("Error trying to get available quantity", e);
							}
							countdownLatch.countDown();
						}
					};
					executorService.submit(r);
				}
				try
				{
					countdownLatch.await();
				}
				catch (final InterruptedException e)
				{
					logger.warn("Thread execution interruptted");
				}
				return finalResp;
			}
			else
			{
				throw new RuntimeException("Not a sharded jedis");
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	@Override
	public GetQuantityResponse getInstockQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get Instock Quantity Request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final GetQuantityResponse finalResp = new GetQuantityResponse();
				final Map<Jedis, LocationProducts> splitRes = ShardStockUtils.splitLocProdByShard(locProds, (ShardedJedis) jedis);
				final CountDownLatch countdownLatch = new CountDownLatch(splitRes.size());
				for (final Jedis nextShard : splitRes.keySet())
				{
					final Runnable r = new Runnable()
					{
						@Override
						public void run()
						{
							try {
								final LocationProducts reqPerShard = splitRes.get(nextShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial get quantity request for shard: " + nextShard + " is: " + reqPerShard);
								}
								final GetQuantityResponse shardResp = redisStockOperationTemplate.getInstockQuantities(nextShard,
										reqPerShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial get quantity response for shard: " + nextShard + " is: " + shardResp);
								}
								finalResp.mergeResponse(shardResp);
							}
							catch (final Exception e)
							{
								logger.error("Error trying to get instock quantity", e);
							}
							countdownLatch.countDown();
						}
					};
					executorService.submit(r);
				}
				try
				{
					countdownLatch.await();
				}
				catch (final InterruptedException e)
				{
					logger.warn("Thread execution interruptted");
				}
				return finalResp;
			}
			else
			{
				throw new RuntimeException("Not a sharded jedis");
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	@Override
	public GetQuantityResponse getReservedQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get Reserved Quantity Request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final GetQuantityResponse finalResp = new GetQuantityResponse();
				final Map<Jedis, LocationProducts> splitRes = ShardStockUtils.splitLocProdByShard(locProds, (ShardedJedis) jedis);
				final CountDownLatch countdownLatch = new CountDownLatch(splitRes.size());
				for (final Jedis nextShard : splitRes.keySet())
				{
					final Runnable r = new Runnable()
					{
						@Override
						public void run()
						{
							try {
								final LocationProducts reqPerShard = splitRes.get(nextShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial get quantity request for shard: " + nextShard + " is: " + reqPerShard);
								}
								final GetQuantityResponse shardResp = redisStockOperationTemplate.getReservedQuantities(nextShard,
										reqPerShard);
								if (logger.isDebugEnabled())
								{
									logger.debug("Partial get quantity response for shard: " + nextShard + " is: " + shardResp);
								}
								finalResp.mergeResponse(shardResp);
							}
							catch (final Exception e)
							{
								logger.error("Error trying to get reserved quantity", e);
							}
							countdownLatch.countDown();
						}
					};
					executorService.submit(r);
				}
				try
				{
					countdownLatch.await();
				}
				catch (final InterruptedException e)
				{
					logger.warn("Thread execution interruptted");
				}
				return finalResp;
			}
			else
			{
				throw new RuntimeException("Not a sharded jedis");
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	@Override
	public int getInstockQuantity(final String locationId, final String prodCode)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get instock quantity request, location id: " + locationId + " product code: " + prodCode);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.getInstockQuantity(jedis, locationId, prodCode);
			}
			else
			{
				throw new RuntimeException("Not a simple jedis");
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	@Override
	public int getReservedQuantity(final String locationId, final String prodCode)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get reserved quantity request, location id: " + locationId + " product code: " + prodCode);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.getReservedQuantity(jedis, locationId, prodCode);
			}
			else
			{
				throw new RuntimeException("Not a simple jedis");
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	@Override
	public int getAvailableQuantity(final String locationId, final String prodCode)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get available quantity request, location id: " + locationId + " product code: " + prodCode);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.getAvailableQuantity(jedis, locationId, prodCode);
			}
			else
			{
				throw new RuntimeException("Not a simple jedis");
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}
}
