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
import org.wispersd.ordermanagement.sourcing.stock.ReserveStockLevelResponse;
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
public class ReservePartialQtyTask extends AbstractParallelStockCallable<ReserveStockLevelRequest, ReserveStockLevelResponse>
{
	public ReservePartialQtyTask(final TaskExecutionStrategy taskExecutionStrategy, final JedisCommands jedis,
			final ReserveStockLevelRequest originalReq, final RedisStockOperationTemplate redisStockOperationTemplate,
			final boolean splittableBySize, final int maxSize)
	{
		super(taskExecutionStrategy, jedis, originalReq, redisStockOperationTemplate, splittableBySize, maxSize);
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
	protected AbstractParallelStockCallable<ReserveStockLevelRequest, ReserveStockLevelResponse> createSubTask(final Jedis nextShard,
			final ReserveStockLevelRequest subReq)
	{
		return new ReservePartialQtyTask(taskExecutionStrategy, nextShard, subReq, redisStockOperationTemplate, splittableBySize,
				maxSize);
	}


	@Override
	protected ReserveStockLevelResponse execute()
	{
		return redisStockOperationTemplate.reservePartialQuantities((Jedis) jedis, originalReq);
	}

	@Override
	protected int getSize(final ReserveStockLevelRequest originalReq)
	{
		return originalReq.size();
	}

	@Override
	protected ReserveStockLevelResponse createFinalRes()
	{
		return new ReserveStockLevelResponse();
	}

	@Override
	protected void merge(final ReserveStockLevelResponse finalRes, final ReserveStockLevelResponse curRes)
	{
		finalRes.mergeResponse(curRes);
	}



}
