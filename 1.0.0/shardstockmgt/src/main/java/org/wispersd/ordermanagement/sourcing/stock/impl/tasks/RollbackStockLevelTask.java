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

import java.util.List;
import java.util.Map;

import org.wispersd.ordermanagement.sourcing.stock.AbstractStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.ShardStockUtils;
import org.wispersd.ordermanagement.sourcing.stock.impl.RedisStockOperationTemplate;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ShardedJedis;


/**
 *
 */
public class RollbackStockLevelTask extends AbstractParallelStockRunnable<AbstractStockLevelRequest<Integer>>
{
	public RollbackStockLevelTask(final TaskExecutionStrategy taskExecutionStrategy, final JedisCommands jedis,
			final AbstractStockLevelRequest<Integer> originalReq, final RedisStockOperationTemplate redisStockOperationTemplate,
			final boolean splittableBySize, final int maxSize)
	{
		super(taskExecutionStrategy, jedis, originalReq, redisStockOperationTemplate, splittableBySize, maxSize);
	}

	@Override
	protected Map<Jedis, AbstractStockLevelRequest<Integer>> splitRequestByShard()
	{
		return (Map<Jedis, AbstractStockLevelRequest<Integer>>) ShardStockUtils.splitRequestByShard(originalReq,
				(ShardedJedis) jedis);
	}

	@Override
	protected List<AbstractStockLevelRequest<Integer>> splitRequestBySize()
	{
		try
		{
			return (List<AbstractStockLevelRequest<Integer>>) ShardStockUtils.splitRequestBySize(originalReq, maxSize);
		}
		catch (final Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	protected AbstractParallelStockRunnable<AbstractStockLevelRequest<Integer>> createSubTask(final JedisCommands shard,
			final AbstractStockLevelRequest<Integer> subReq)
	{
		return new RollbackStockLevelTask(taskExecutionStrategy, shard, subReq, redisStockOperationTemplate, splittableBySize,
				maxSize);
	}

	@Override
	protected void execute()
	{
		redisStockOperationTemplate.rollbackQuantities((Jedis) jedis, originalReq);
	}

	@Override
	protected int getSize(final AbstractStockLevelRequest<Integer> originalReq)
	{
		return originalReq.size();
	}

}
