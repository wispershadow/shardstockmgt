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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wispersd.ordermanagement.sourcing.stock.impl.RedisStockOperationTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;




import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ShardedJedis;


/**
 *
 */
public abstract class AbstractParallelStockCallable<T, V> implements Callable<V>
{

	protected static final Logger logger = LoggerFactory.getLogger(AbstractParallelStockCallable.class);
	protected final TaskExecutionStrategy taskExecutionStrategy;
	protected final JedisCommands jedis;
	protected final T originalReq;
	protected final RedisStockOperationTemplate redisStockOperationTemplate;
	protected final boolean splittableBySize;
	protected final int maxSize;

	/**
	 *
	 */
	public AbstractParallelStockCallable(final TaskExecutionStrategy taskExecutionStrategy, final JedisCommands jedis,
			final T originalReq, final RedisStockOperationTemplate redisStockOperationTemplate, final boolean splittableBySize,
			final int maxSize)
	{
		this.taskExecutionStrategy = taskExecutionStrategy;
		this.jedis = jedis;
		this.originalReq = originalReq;
		this.redisStockOperationTemplate = redisStockOperationTemplate;
		this.splittableBySize = splittableBySize;
		this.maxSize = maxSize;
	}

	
	public V call()
	{
		try
		{
			if (jedis instanceof ShardedJedis)
			{
				final Map<Jedis, T> splitRes = splitRequestByShard();
				final List<AbstractParallelStockCallable<T, V>> subTasks = new ArrayList<AbstractParallelStockCallable<T, V>>();
				for (final Jedis nextShard : splitRes.keySet())
				{
					final T subReq = splitRes.get(nextShard);
					if (logger.isDebugEnabled())
					{
						logger.debug(originalReq.getClass().getName() + " for shard: " + nextShard + " is: " + subReq);
					}
					final AbstractParallelStockCallable<T, V> subAction = createSubTask(nextShard, subReq);
					subTasks.add(subAction);
				}
				final V finalRes = createFinalRes();
				return taskExecutionStrategy.executeAllCallables(finalRes, subTasks);
			}
			else if (splittableBySize && getSize(originalReq) > maxSize)
			{
				final List<T> splitRes = splitRequestBySize();
				final List<AbstractParallelStockCallable<T, V>> subTasks = new ArrayList<AbstractParallelStockCallable<T, V>>();
				for (final T subReq : splitRes)
				{
					if (logger.isDebugEnabled())
					{
						logger.debug(originalReq.getClass().getName() + " for size: " + subReq);
					}
					final AbstractParallelStockCallable<T, V> subAction = createSubTask((Jedis) jedis, subReq);
					subTasks.add(subAction);
				}
				final V finalRes = createFinalRes();
				return taskExecutionStrategy.executeAllCallables(finalRes, subTasks);
			}
			else
			{
				return execute();
			}

		}
		catch (final Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 *
	 */
	protected abstract int getSize(final T originalReq);

	protected abstract AbstractParallelStockCallable<T, V> createSubTask(Jedis nextShard, T subReq);

	protected abstract Map<Jedis, T> splitRequestByShard();

	protected abstract List<T> splitRequestBySize();

	protected abstract V createFinalRes();

	protected abstract void merge(V finalRes, V curRes);

	protected abstract V execute();
}
