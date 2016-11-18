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
package org.wispersd.ordermanagement.sourcing.stock.impl.tasks;

import org.wispersd.ordermanagement.sourcing.stock.ReserveStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.ShardStockUtils;
import org.wispersd.ordermanagement.sourcing.stock.impl.RedisStockOperationTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ShardedJedis;


/**
 *
 */
public class ReserveAllQtyTask extends AbstractParallelStockCallable<ReserveStockLevelRequest, List<Object>>
{
	public ReserveAllQtyTask(final TaskExecutionStrategy taskExecutionStrategy, final JedisCommands jedis,
			final ReserveStockLevelRequest originalReq, final RedisStockOperationTemplate redisStockOperationTemplate,
			final boolean splittableBySize, final int maxSize)
	{
		super(taskExecutionStrategy, jedis, originalReq, redisStockOperationTemplate, splittableBySize, maxSize);
	}

	@Override
	protected AbstractParallelStockCallable<ReserveStockLevelRequest, List<Object>> createSubTask(final Jedis nextShard,
			final ReserveStockLevelRequest subReq)
	{
		return new ReserveAllQtyTask(taskExecutionStrategy, nextShard, subReq, redisStockOperationTemplate, splittableBySize,
				maxSize);
	}

	@Override
	protected Map<Jedis, ReserveStockLevelRequest> splitRequestByShard()
	{
		return (Map<Jedis, ReserveStockLevelRequest>) ShardStockUtils.splitRequestByShard(originalReq, (ShardedJedis) jedis);
	}

	@Override
	protected List<ReserveStockLevelRequest> splitRequestBySize()
	{
		try
		{
			return (List<ReserveStockLevelRequest>) ShardStockUtils.splitRequestBySize(originalReq, maxSize);
		}
		catch (final Exception e)
		{
			throw new RuntimeException(e);
		}
	}


	@Override
	protected List execute()
	{
		final List res = new ArrayList(3);
		res.add(jedis);
		res.add(originalReq);
		try
		{
			final boolean opRes = redisStockOperationTemplate.reserveAllQuantities((Jedis) jedis, originalReq);
			res.add(Boolean.valueOf(opRes));
			if (!opRes)
			{
				logger.warn("reservation failed for request: " + originalReq + " shard: " + jedis);
			}
		}
		catch (final Exception e)
		{
			logger.error("Error trying to reserve quantity: ", e);
			res.add(Boolean.valueOf(false));
		}
		return res;
	}

	@Override
	protected int getSize(final ReserveStockLevelRequest originalReq)
	{
		return originalReq.size();
	}

	@Override
	protected List<Object> createFinalRes()
	{
		return new ArrayList<Object>();
	}

	@Override
	protected void merge(final List<Object> finalRes, final List<Object> curRes)
	{
		synchronized (finalRes)
		{
			finalRes.add(curRes);
		}
	}

}
