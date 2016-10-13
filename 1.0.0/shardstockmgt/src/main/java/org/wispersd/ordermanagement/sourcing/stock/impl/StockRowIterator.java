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
package org.wispersd.ordermanagement.sourcing.stock.impl;

import java.util.Iterator;
import java.util.Map;

import org.wispersd.ordermanagement.sourcing.stock.StockRow;


/**
 *
 */
public class StockRowIterator<T> implements Iterator<StockRow<T>>
{
	private final Map<String, Map<String, T>> reqMap;
	private final Iterator<String> locationIdIter;
	private String curLocationId = null;
	private Iterator<Map.Entry<String, T>> prodValueIter;

	/**
	 *
	 */
	public StockRowIterator(final Map<String, Map<String, T>> reqMap)
	{
		this.reqMap = reqMap;
		this.locationIdIter = reqMap.keySet().iterator();
		if (locationIdIter.hasNext())
		{
			curLocationId = locationIdIter.next();
			prodValueIter = reqMap.get(curLocationId).entrySet().iterator();
		}
	}

	@Override
	public boolean hasNext()
	{
		if (prodValueIter == null)
		{
			return false;
		}
		else
		{
			if (prodValueIter.hasNext())
			{
				return true;
			}
			else
			{
				while (!prodValueIter.hasNext())
				{
					if (locationIdIter.hasNext())
					{
						curLocationId = locationIdIter.next();
						prodValueIter = reqMap.get(curLocationId).entrySet().iterator();
					}
					else
					{
						return false;
					}
				}
				return true;
			}
		}
	}

	@Override
	public StockRow<T> next()
	{
		if (!hasNext())
		{
			return null;
		}
		final Map.Entry<String, T> curProdValue = prodValueIter.next();
		return new StockRow(curLocationId, curProdValue.getKey(), curProdValue.getValue());
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException("deleting stock row iterator is not supported");

	}

}
