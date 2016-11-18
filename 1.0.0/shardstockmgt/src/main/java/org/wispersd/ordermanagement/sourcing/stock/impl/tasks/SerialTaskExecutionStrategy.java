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


/**
 *
 */
public class SerialTaskExecutionStrategy implements TaskExecutionStrategy
{

	public <T> void executeAllRunnables(final List<AbstractParallelStockRunnable<T>> actions)
	{
		for (final AbstractParallelStockRunnable<T> nextAction : actions)
		{
			nextAction.run();
		}
	}

	public <T, V> V executeAllCallables(final V finalRes, final List<AbstractParallelStockCallable<T, V>> tasks)
	{
		for (final AbstractParallelStockCallable<T, V> nextTask : tasks)
		{
			final V nextRes = nextTask.call();
			nextTask.merge(finalRes, nextRes);
		}
		return finalRes;
	}

}
