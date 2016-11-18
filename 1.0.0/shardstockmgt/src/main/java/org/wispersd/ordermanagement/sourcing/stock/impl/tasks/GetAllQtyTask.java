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

import org.wispersd.ordermanagement.sourcing.stock.LocationProducts;
import org.wispersd.ordermanagement.sourcing.stock.MultiGetQuantityResponse;
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
public class GetAllQtyTask extends AbstractParallelStockCallable<LocationProducts, MultiGetQuantityResponse>
{
	public GetAllQtyTask(final TaskExecutionStrategy taskExecutionStrategy, final JedisCommands jedis,
			final LocationProducts originalReq, final RedisStockOperationTemplate redisStockOperationTemplate,
			final boolean splittableBySize, final int maxSize)
	{
		super(taskExecutionStrategy, jedis, originalReq, redisStockOperationTemplate, splittableBySize, maxSize);
	}

	@Override
	protected AbstractParallelStockCallable<LocationProducts, MultiGetQuantityResponse> createSubTask(final Jedis nextShard,
			final LocationProducts subReq)
	{
		return new GetAllQtyTask(taskExecutionStrategy, nextShard, subReq, redisStockOperationTemplate, splittableBySize, maxSize);
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
	protected MultiGetQuantityResponse execute()
	{
		return redisStockOperationTemplate.getAllQuantities((Jedis) jedis, originalReq);
	}

	@Override
	protected int getSize(final LocationProducts originalReq)
	{
		return originalReq.size();
	}

	@Override
	protected MultiGetQuantityResponse createFinalRes()
	{
		return new MultiGetQuantityResponse();
	}

	@Override
	protected void merge(final MultiGetQuantityResponse finalRes, final MultiGetQuantityResponse curRes)
	{
		finalRes.mergeResponse(curRes);
	}

}
