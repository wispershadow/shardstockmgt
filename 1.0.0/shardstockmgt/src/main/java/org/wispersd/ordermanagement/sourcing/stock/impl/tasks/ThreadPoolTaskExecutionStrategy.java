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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *
 */
public class ThreadPoolTaskExecutionStrategy implements TaskExecutionStrategy
{
	private static final Logger logger = LoggerFactory.getLogger(ThreadPoolTaskExecutionStrategy.class);
	private ExecutorService executorService;


	/**
	 * @return the executorService
	 */
	public ExecutorService getExecutorService()
	{
		return executorService;
	}

	/**
	 * @param executorService
	 *           the executorService to set
	 */
	public void setExecutorService(final ExecutorService executorService)
	{
		this.executorService = executorService;
	}

	public <T> void executeAllRunnables(final List<AbstractParallelStockRunnable<T>> actions)
	{
		final CountDownLatch latch = new CountDownLatch(actions.size());
		for (int i = 0; i < actions.size(); i++)
		{
			final AbstractParallelStockRunnable<T> curTask = actions.get(i);
			final Runnable r = new Runnable()
			{
				public void run()
				{
					curTask.run();
					latch.countDown();
				}
			};
			executorService.submit(r);
		}
		try
		{
			latch.await();
		}
		catch (final InterruptedException e)
		{
			logger.warn("Thread execution interruptted");
		}
	}

	
	public <T, V> V executeAllCallables(final V finalRes, final List<AbstractParallelStockCallable<T, V>> tasks)
	{
		final CountDownLatch latch = new CountDownLatch(tasks.size());
		for (int i = 0; i < tasks.size(); i++)
		{
			final AbstractParallelStockCallable<T, V> curTask = tasks.get(i);
			final Callable<V> c = new Callable<V>()
			{
				public V call()
				{
					final V curRes = curTask.call();
					curTask.merge(finalRes, curRes);
					latch.countDown();
					return curRes;
				}
			};
			executorService.submit(c);
		}
		try
		{
			latch.await();
		}
		catch (final InterruptedException e)
		{
			logger.warn("Thread execution interruptted");
		}
		return finalRes;
	}

}
