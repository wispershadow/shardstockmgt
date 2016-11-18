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
package org.wispersd.ordermanagement.sourcing.stock;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.wispersd.ordermanagement.sourcing.stock.impl.StockRowIterator;


/**
 *
 */
public class AbstractStockLevelResponse<T>
{
	private final ConcurrentMap<String, Map<String, T>> respMap = new ConcurrentHashMap<String, Map<String, T>>();

	public int size()
	{
		return respMap.size();
	}

	public boolean isEmpty()
	{
		return respMap.isEmpty();
	}

	public void clear()
	{
		respMap.clear();
	}

	public void addStockLevel(final String locationId, final String prodCode, final T data)
	{
		Map<String, T> prodValMap = respMap.get(locationId);
		if (prodValMap == null)
		{
			final Map<String, T> tmpMap = new ConcurrentHashMap<String, T>();
			prodValMap = respMap.putIfAbsent(locationId, tmpMap);
			if (prodValMap == null)
			{
				prodValMap = tmpMap;
			}
		}
		prodValMap.put(prodCode, data);
	}


	public T getStockLevel(final String locationId, final String prodCode)
	{
		final Map<String, T> prodValMap = respMap.get(locationId);
		if (prodValMap == null)
		{
			return null;
		}
		else
		{
			return prodValMap.get(prodCode);
		}
	}


	public Iterator<String> getAllLocations()
	{
		return respMap.keySet().iterator();
	}

	public Iterator<String> getProductsForLocation(final String locationId)
	{
		Map<String, T> prodValMap = respMap.get(locationId);
		if (prodValMap == null)
		{
			prodValMap = Collections.emptyMap();
		}
		return prodValMap.keySet().iterator();
	}

	public Iterator<StockRow<T>> getStockRowIterator()
	{
		return new StockRowIterator<T>(respMap);
	}

	public void mergeResponse(final AbstractStockLevelResponse<T> shardResponse)
	{
		final Iterator<StockRow<T>> shardRowIter = shardResponse.getStockRowIterator();
		while (shardRowIter.hasNext())
		{
			final StockRow<T> nextStockRow = shardRowIter.next();
			this.addStockLevel(nextStockRow.getLocationId(), nextStockRow.getProdCode(), nextStockRow.getValue());
		}
	}


	@Override
	public String toString()
	{
		return "StockLevelResponse [reqMap=" + respMap + "]";
	}

	public Map<String, Map<String, T>> toMap()
	{
		return Collections.unmodifiableMap(respMap);
	}
}
