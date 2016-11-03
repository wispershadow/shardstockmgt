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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wispersd.core.data.nosql.redis.JedisDataManager;
import org.wispersd.core.data.nosql.redis.JedisUtils;
import org.wispersd.ordermanagement.sourcing.stock.AddStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.CommitStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.CommitStockLevelResponse;
import org.wispersd.ordermanagement.sourcing.stock.GetQuantityResponse;
import org.wispersd.ordermanagement.sourcing.stock.LocationProducts;
import org.wispersd.ordermanagement.sourcing.stock.ReserveStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.ReserveStockLevelResponse;
import org.wispersd.ordermanagement.sourcing.stock.RollbackStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.StockManager;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;


/**
 *
 */
public class RedisStockManagerImpl implements StockManager
{
	private static final Logger logger = LoggerFactory.getLogger(RedisStockManagerImpl.class);
	private JedisDataManager jedisDataManager;
	private static final String TABLENAME = "stock";
	private RedisStockOperationTemplate redisStockOperationTemplate;


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

	@Override
	public void clearAllStockData()
	{
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				((Jedis) jedis).flushDB();
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
			if (jedis instanceof Jedis)
			{
				redisStockOperationTemplate.addStockLevelData((Jedis) jedis, stockLevelReq);
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
	public void updateStockLevelData(final UpdateStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Update Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				redisStockOperationTemplate.updateStockLevelData((Jedis) jedis, stockLevelReq);
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
	public boolean reserveAllQuantities(final ReserveStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Reserve Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.reserveAllQuantities((Jedis) jedis, stockLevelReq);
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
	public ReserveStockLevelResponse reservePartialQuantities(final ReserveStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Reserve Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.reservePartialQuantities((Jedis) jedis, stockLevelReq);
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
	public boolean commitAllQuantities(final CommitStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Commit Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.commitAllQuantities((Jedis) jedis, stockLevelReq);
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
	public CommitStockLevelResponse commitPartitalQuantities(final CommitStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Commit Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.commitPartialQuantities((Jedis) jedis, stockLevelReq);
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
	public void rollbackQuantities(final RollbackStockLevelRequest stockLevelReq)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Rollback Stock Level Request is: " + stockLevelReq);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				redisStockOperationTemplate.rollbackQuantities((Jedis) jedis, stockLevelReq);
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
	public GetQuantityResponse getInstockQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get instock quantity request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.getInstockQuantities((Jedis) jedis, locProds);
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
	public GetQuantityResponse getReservedQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get reserved quantity request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.getReservedQuantities((Jedis) jedis, locProds);
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
	public GetQuantityResponse getAvailableQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get available quantity request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.getAvailableQuantities((Jedis) jedis, locProds);
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

	@Override
	public MultiGetQuantityResponse getAllQuantities(final LocationProducts locProds)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get all quantity request is: " + locProds);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
			{
				return redisStockOperationTemplate.getAllQuantities((Jedis) jedis, locProds);
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
	public int[] getAllQuantities(final String locationId, final String prodCode)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("Get all quantity request, location id: " + locationId + " product code: " + prodCode);
		}
		final JedisCommands jedis = jedisDataManager.getJedisByTableName(TABLENAME);
		try
		{
			if (jedis instanceof Jedis)
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
