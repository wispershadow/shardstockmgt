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
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;


/**
 *
 */
public class ShardStockUtils
{
	public static <T> Map<Jedis, ? extends AbstractStockLevelRequest<T>> splitRequestByShard(
			final AbstractStockLevelRequest<T> original, final ShardedJedis shardedJedis)
	{
		final Map<Jedis, AbstractStockLevelRequest<T>> splitReqMap = new HashMap<Jedis, AbstractStockLevelRequest<T>>();
		final Iterator<StockRow<T>> stockRowIter = original.getStockRowIterator();
		while (stockRowIter.hasNext())
		{
			final StockRow<T> nextStockRow = stockRowIter.next();
			final Jedis jedis = shardedJedis.getShard(nextStockRow.getLocationId());
			AbstractStockLevelRequest<T> req = splitReqMap.get(jedis);
			try
			{
				if (req == null)
				{
					req = original.getClass().newInstance();
					splitReqMap.put(jedis, req);
				}
				req.addStockLevel(nextStockRow.getLocationId(), nextStockRow.getProdCode(), nextStockRow.getValue());
			}
			catch (final Exception e)
			{
			}
		}

		return splitReqMap;
	}


	public static <T> List<? extends AbstractStockLevelRequest<T>> splitRequestBySize(final AbstractStockLevelRequest<T> original,
			final int splitSize) throws Exception
	{
		if (original.isEmpty())
		{
			return Collections.singletonList(original);
		}
		final List<AbstractStockLevelRequest<T>> result = new ArrayList<AbstractStockLevelRequest<T>>();
		final Iterator<StockRow<T>> stockRowIter = original.getStockRowIterator();
		int count = 0;
		AbstractStockLevelRequest<T> req = original.getClass().newInstance();
		while (stockRowIter.hasNext())
		{
			final StockRow<T> nextStockRow = stockRowIter.next();
			req.addStockLevel(nextStockRow.getLocationId(), nextStockRow.getProdCode(), nextStockRow.getValue());
			count++;
			if (count == splitSize)
			{
				result.add(req);
				req = original.getClass().newInstance();
				count = 0;
			}
		}
		if (count > 0)
		{
			result.add(req);
		}
		return result;
	}


	public static Map<Jedis, LocationProducts> splitLocProdByShard(final LocationProducts locProds, final ShardedJedis shardedJedis)
	{
		final Set<String> allLocs = locProds.getAllLocations();
		final Map<Jedis, LocationProducts> splitReqMap = new HashMap<Jedis, LocationProducts>();
		for (final String nextLoc : allLocs)
		{
			final Set<String> prodsForLoc = locProds.getProductsForLocation(nextLoc);
			final Jedis jedis = shardedJedis.getShard(nextLoc);
			LocationProducts locProdCurShard = splitReqMap.get(jedis);
			if (locProdCurShard == null)
			{
				locProdCurShard = new LocationProducts();
				splitReqMap.put(jedis, locProdCurShard);
			}
			locProdCurShard.addProductsForLocation(nextLoc, prodsForLoc);
		}
		return splitReqMap;
	}


	public static List<LocationProducts> splitLocProdBySize(final LocationProducts locProds, final int splitSize)
	{
		final Set<String> allLocs = locProds.getAllLocations();
		int count = 0;
		final List<LocationProducts> result = new ArrayList<LocationProducts>();
		LocationProducts curLocProd = new LocationProducts();
		for (final String nextLoc : allLocs)
		{
			final Set<String> prodsForLoc = locProds.getProductsForLocation(nextLoc);
			for (final String nextProd : prodsForLoc)
			{
				curLocProd.addProductForLocation(nextLoc, nextProd);
				count++;
				if (count == splitSize)
				{
					result.add(curLocProd);
					curLocProd = new LocationProducts();
					count = 0;
				}
			}
		}
		if (count > 0)
		{
			result.add(curLocProd);
		}
		return result;
	}
}
