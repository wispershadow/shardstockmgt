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

import org.wispersd.ordermanagement.sourcing.stock.GetQuantityResponse;
import org.wispersd.ordermanagement.sourcing.stock.LocationProducts;
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
public class GetReservedQtyTask extends AbstractParallelStockCallable<LocationProducts, GetQuantityResponse>
{
	public GetReservedQtyTask(final TaskExecutionStrategy taskExecutionStrategy, final JedisCommands jedis,
			final LocationProducts originalReq, final RedisStockOperationTemplate redisStockOperationTemplate,
			final boolean splittableBySize, final int maxSize)
	{
		super(taskExecutionStrategy, jedis, originalReq, redisStockOperationTemplate, splittableBySize, maxSize);
	}

	@Override
	protected AbstractParallelStockCallable<LocationProducts, GetQuantityResponse> createSubTask(final Jedis nextShard,
			final LocationProducts subReq)
	{
		return new GetReservedQtyTask(taskExecutionStrategy, nextShard, subReq, redisStockOperationTemplate, splittableBySize,
				maxSize);
	}

	@Override
	protected Map<Jedis, LocationProducts> splitRequestByShard()
	{
		return ShardStockUtils.splitLocProdByShard(originalReq, (ShardedJedis) jedis);
	}

	@Override
	protected List<LocationProducts> splitRequestBySize()
	{
		return ShardStockUtils.splitLocProdBySize(originalReq, maxSize);
	}


	@Override
	protected GetQuantityResponse execute()
	{
		return redisStockOperationTemplate.getReservedQuantities((Jedis) jedis, originalReq);
	}

	@Override
	protected int getSize(final LocationProducts originalReq)
	{
		return originalReq.size();
	}

	@Override
	protected GetQuantityResponse createFinalRes()
	{
		return new GetQuantityResponse();
	}

	@Override
	protected void merge(final GetQuantityResponse finalRes, final GetQuantityResponse curRes)
	{
		finalRes.mergeResponse(curRes);
	}

}
