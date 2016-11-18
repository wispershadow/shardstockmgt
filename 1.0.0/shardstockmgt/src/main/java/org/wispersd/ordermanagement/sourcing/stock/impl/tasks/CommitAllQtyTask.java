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

import org.wispersd.ordermanagement.sourcing.stock.CommitStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.ShardStockUtils;
import org.wispersd.ordermanagement.sourcing.stock.impl.RedisStockOperationTemplate;

import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ShardedJedis;


/**
 *
 */
public class CommitAllQtyTask extends AbstractParallelStockRunnable<CommitStockLevelRequest>
{

	public CommitAllQtyTask(final TaskExecutionStrategy taskExecutionStrategy, final JedisCommands jedis,
			final CommitStockLevelRequest originalReq, final RedisStockOperationTemplate redisStockOperationTemplate,
			final boolean splittableBySize, final int maxSize)
	{
		super(taskExecutionStrategy, jedis, originalReq, redisStockOperationTemplate, splittableBySize, maxSize);
	}

	@Override
	protected Map<Jedis, CommitStockLevelRequest> splitRequestByShard()
	{
		return (Map<Jedis, CommitStockLevelRequest>) ShardStockUtils.splitRequestByShard(originalReq, (ShardedJedis) jedis);
	}

	@Override
	protected List<CommitStockLevelRequest> splitRequestBySize()
	{
		try
		{
			return (List<CommitStockLevelRequest>) ShardStockUtils.splitRequestBySize(originalReq, maxSize);
		}
		catch (final Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	protected AbstractParallelStockRunnable<CommitStockLevelRequest> createSubTask(final JedisCommands shard,
			final CommitStockLevelRequest subReq)
	{
		return new CommitAllQtyTask(taskExecutionStrategy, shard, subReq, redisStockOperationTemplate, splittableBySize, maxSize);
	}

	@Override
	protected void execute()
	{
		redisStockOperationTemplate.commitAllQuantities((Jedis) jedis, originalReq);
	}

	@Override
	protected int getSize(final CommitStockLevelRequest originalReq)
	{
		return originalReq.size();
	}

}
