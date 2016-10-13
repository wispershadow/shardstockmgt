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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.wispersd.ordermanagement.sourcing.stock.impl.StockRowIterator;


/**
 *
 */
public class AbstractStockLevelRequest<T>
{
	private final Map<String, Map<String, T>> reqMap = new HashMap<String, Map<String, T>>();

	public void addStockLevel(final String locationId, final String prodCode, final T data)
	{
		Map<String, T> prodValMap = reqMap.get(locationId);
		if (prodValMap == null)
		{
			prodValMap = new HashMap<String, T>();
			reqMap.put(locationId, prodValMap);
		}
		prodValMap.put(prodCode, data);
	}

	public T getStockLevel(final String locationId, final String prodCode)
	{
		final Map<String, T> prodValMap = reqMap.get(locationId);
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
		return reqMap.keySet().iterator();
	}

	public Iterator<String> getProductsForLocation(final String locationId)
	{
		Map<String, T> prodValMap = reqMap.get(locationId);
		if (prodValMap == null)
		{
			prodValMap = Collections.emptyMap();
		}
		return prodValMap.keySet().iterator();
	}

	public Iterator<StockRow<T>> getStockRowIterator()
	{
		return new StockRowIterator<T>(reqMap);
	}

	public List<String> toScirptParameters()
	{
		final List<String> result = new ArrayList<String>();
		final StringBuilder locationIdsBuilder = new StringBuilder();
		final StringBuilder prodCodesBuilder = new StringBuilder();
		final StringBuilder quantitiesBuilder = new StringBuilder();
		for (final String nextLocId : reqMap.keySet())
		{
			final Map<String, T> prodValMap = reqMap.get(nextLocId);
			for (final String nextProdCode : prodValMap.keySet())
			{
				final T nextData = prodValMap.get(nextProdCode);
				if (locationIdsBuilder.length() > 0)
				{
					locationIdsBuilder.append(",");
					prodCodesBuilder.append(",");
					quantitiesBuilder.append(",");
				}
				locationIdsBuilder.append(nextLocId);
				prodCodesBuilder.append(nextProdCode);
				quantitiesBuilder.append(nextData.toString());
			}
		}
		result.add(locationIdsBuilder.toString());
		result.add(prodCodesBuilder.toString());
		result.add(quantitiesBuilder.toString());
		return result;
	}

	@Override
	public String toString()
	{
		return "StockLevelRequest [reqMap=" + reqMap + "]";
	}
}
