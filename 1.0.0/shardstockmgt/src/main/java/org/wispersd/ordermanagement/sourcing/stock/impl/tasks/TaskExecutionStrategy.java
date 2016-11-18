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
public interface TaskExecutionStrategy
{
	public <T> void executeAllRunnables(List<AbstractParallelStockRunnable<T>> actions);

	public <T, V> V executeAllCallables(V finalRes, List<AbstractParallelStockCallable<T, V>> tasks);
}
