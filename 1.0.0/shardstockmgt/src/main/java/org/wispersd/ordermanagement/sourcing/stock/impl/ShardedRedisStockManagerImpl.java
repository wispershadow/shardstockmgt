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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wispersd.core.data.nosql.redis.JedisDataManager;
import org.wispersd.core.data.nosql.redis.JedisUtils;
import org.wispersd.ordermanagement.sourcing.stock.AddStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.CommitStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.CommitStockLevelResponse;
import org.wispersd.ordermanagement.sourcing.stock.GetQuantityResponse;
import org.wispersd.ordermanagement.sourcing.stock.LocationProducts;
import org.wispersd.ordermanagement.sourcing.stock.MultiGetQuantityResponse;
import org.wispersd.ordermanagement.sourcing.stock.ReserveStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.ReserveStockLevelResponse;
import org.wispersd.ordermanagement.sourcing.stock.RollbackStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.StockManager;
import org.wispersd.ordermanagement.sourcing.stock.UpdateStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.AddStockLevelTask;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.CommitAllQtyTask;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.CommitPartialQtyTask;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.GetAllQtyTask;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.GetAvailableQtyTask;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.GetInstockQtyTask;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.GetReservedQtyTask;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.ReserveAllQtyTask;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.ReservePartialQtyTask;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.RollbackStockLevelTask;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.TaskExecutionStrategy;
import org.wispersd.ordermanagement.sourcing.stock.impl.tasks.UpdateStockLevelTask;

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
	private TaskExecutionStrategy taskExecutionStrategy;
	private int maxSplitSize = 500;

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

	/**
	 * @return the taskExecutionStrategy
	 */
	public TaskExecutionStrategy getTaskExecutionStrategy()
	{
		return taskExecutionStrategy;
	}

	/**
	 * @param taskExecutionStrategy
	 *           the taskExecutionStrategy to set
	 */
	public void setTaskExecutionStrategy(final TaskExecutionStrategy taskExecutionStrategy)
	{
		this.taskExecutionStrategy = taskExecutionStrategy;
	}


	/**
	 * @return the maxSplitSize
	 */
	public int getMaxSplitSize()
	{
		return maxSplitSize;
	}

	/**
	 * @param maxSplitSize
	 *           the maxSplitSize to set
	 */
	public void setMaxSplitSize(final int maxSplitSize)
	{
		this.maxSplitSize = maxSplitSize;
	}

	
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

	
	public void addStockLevelData(final AddStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Add Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final AddStockLevelTask addStockLevelTask = new AddStockLevelTask(taskExecutionStrategy, jedis, stockLevelReq,
					redisStockOperationTemplate, true, maxSplitSize);
			addStockLevelTask.run();
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	
	public void updateStockLevelData(final UpdateStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Update Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final UpdateStockLevelTask updateStockLevelTask = new UpdateStockLevelTask(taskExecutionStrategy, jedis, stockLevelReq,
					redisStockOperationTemplate, true, maxSplitSize);
			updateStockLevelTask.run();
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	
	public boolean reserveAllQuantities(final ReserveStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Reserve Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final ReserveAllQtyTask reserveAllQtyTask = new ReserveAllQtyTask(taskExecutionStrategy, jedis, stockLevelReq,
					redisStockOperationTemplate, false, maxSplitSize);
			final List<Object> reserveRes = reserveAllQtyTask.call();
			final List<Object> toRollback = new ArrayList<Object>();
			final boolean needRollback = filterRollbackTasks(reserveRes, toRollback);
			if (needRollback)
			{
				for (final Object nextToRollback : toRollback)
				{
					final List<Object> converted = (List<Object>) nextToRollback;
					final JedisCommands curShard = (JedisCommands) converted.get(0);
					final ReserveStockLevelRequest originalReq = (ReserveStockLevelRequest) converted.get(1);
					final RollbackStockLevelTask rollbackTask = new RollbackStockLevelTask(taskExecutionStrategy, curShard,
							originalReq, redisStockOperationTemplate, false, maxSplitSize);
					rollbackTask.run();
				}
			}
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
		return false;
	}


	protected boolean filterRollbackTasks(final List<Object> reserveRes, final List<Object> toRollback)
	{
		boolean allSuccess = true;
		for (final Object nextReserveRes : reserveRes)
		{
			final List<Object> converted = (List<Object>) nextReserveRes;
			final JedisCommands jedis = (JedisCommands) converted.get(0);
			final ReserveStockLevelRequest originalReq = (ReserveStockLevelRequest) converted.get(1);
			final Boolean success = (Boolean) converted.get(2);
			if (!success.booleanValue())
			{
				logger.warn("Reservation failed for jedis:" + jedis + " reservation request:" + originalReq);
				allSuccess = false;
			}
			else
			{
				toRollback.add(nextReserveRes);
			}
		}
		if (allSuccess)
		{
			toRollback.clear();
		}
		return !allSuccess;
	}

	
	public ReserveStockLevelResponse reservePartialQuantities(final ReserveStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Reserve Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final ReservePartialQtyTask reservePartialQtyTask = new ReservePartialQtyTask(taskExecutionStrategy, jedis,
					stockLevelReq, redisStockOperationTemplate, false, maxSplitSize);
			return reservePartialQtyTask.call();
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	
	public boolean commitAllQuantities(final CommitStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Commit Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final CommitAllQtyTask commitAllQtyTask = new CommitAllQtyTask(taskExecutionStrategy, jedis, stockLevelReq,
					redisStockOperationTemplate, false, maxSplitSize);
			commitAllQtyTask.run();
			return true;
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	
	public CommitStockLevelResponse commitPartitalQuantities(final CommitStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Commit Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final CommitPartialQtyTask commitPartialQtyTask = new CommitPartialQtyTask(taskExecutionStrategy, jedis, stockLevelReq,
					redisStockOperationTemplate, false, maxSplitSize);
			return commitPartialQtyTask.call();
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	
	public void rollbackQuantities(final RollbackStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Rollback Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final RollbackStockLevelTask rollbackStockLevelTask = new RollbackStockLevelTask(taskExecutionStrategy, jedis,
					stockLevelReq, redisStockOperationTemplate, false, maxSplitSize);
			rollbackStockLevelTask.run();
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	
	public GetQuantityResponse getAvailableQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get Available Quantity Request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final GetAvailableQtyTask getAvailableQtyTask = new GetAvailableQtyTask(taskExecutionStrategy, jedis, locProds,
					redisStockOperationTemplate, true, maxSplitSize);
			return getAvailableQtyTask.call();
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	
	public GetQuantityResponse getInstockQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get Instock Quantity Request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final GetInstockQtyTask getInstockQtyTask = new GetInstockQtyTask(taskExecutionStrategy, jedis, locProds,
					redisStockOperationTemplate, true, maxSplitSize);
			return getInstockQtyTask.call();
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	
	public GetQuantityResponse getReservedQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get Reserved Quantity Request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final GetReservedQtyTask getReservedQtyTask = new GetReservedQtyTask(taskExecutionStrategy, jedis, locProds,
					redisStockOperationTemplate, true, maxSplitSize);
			return getReservedQtyTask.call();
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}


	
	public MultiGetQuantityResponse getAllQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get all quantity request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			final GetAllQtyTask getAllQtyTask = new GetAllQtyTask(taskExecutionStrategy, jedis, locProds,
					redisStockOperationTemplate, true, maxSplitSize);
			return getAllQtyTask.call();
		}
		finally
		{
			JedisUtils.closeJedis(jedis);
		}
	}

	
	public int getInstockQuantity(final String locationId, final String prodCode)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get instock quantity request, location id: " + locationId + " product code: " + prodCode);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
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

	
	public int getReservedQuantity(final String locationId, final String prodCode)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get reserved quantity request, location id: " + locationId + " product code: " + prodCode);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
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

	
	public int getAvailableQuantity(final String locationId, final String prodCode)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get available quantity request, location id: " + locationId + " product code: " + prodCode);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
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


	
	public int[] getAllQuantities(final String locationId, final String prodCode)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get all quantity request, location id: " + locationId + " product code: " + prodCode);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				return redisStockOperationTemplate.getAllQuantity(jedis, locationId, prodCode);
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
