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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.wispersd.ordermanagement.sourcing.stock.AbstractStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.AddStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.CommitStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.CommitStockLevelResponse;
import org.wispersd.ordermanagement.sourcing.stock.GetQuantityResponse;
import org.wispersd.ordermanagement.sourcing.stock.LocationProducts;
import org.wispersd.ordermanagement.sourcing.stock.MultiGetQuantityResponse;
import org.wispersd.ordermanagement.sourcing.stock.RedisFunctions;
import org.wispersd.ordermanagement.sourcing.stock.ReserveStockLevelRequest;
import org.wispersd.ordermanagement.sourcing.stock.ReserveStockLevelResponse;
import org.wispersd.ordermanagement.sourcing.stock.StockQuantities;
import org.wispersd.ordermanagement.sourcing.stock.StockRow;
import org.wispersd.ordermanagement.sourcing.stock.UpdateStockLevelRequest;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;


/**
 *
 */
public class RedisStockOperationTemplate
{
	private static final String SUFFIX_RESERVED = "_r";
	private static final String SUFFIX_INSTOCK = "_i";
	private static final String SUFFIX_OVERSELL = "_o";

	public void addStockLevelData(final Jedis jedis, final AddStockLevelRequest stockLevelReq)
	{
		final Pipeline p = jedis.pipelined();
		final Iterator<StockRow<StockQuantities>> iter = stockLevelReq.getStockRowIterator();
		while (iter.hasNext())
		{
			final StockRow<StockQuantities> curRow = iter.next();
			final StockQuantities stockQtys = curRow.getValue();
			p.hincrBy(curRow.getLocationId(), curRow.getProdCode() + SUFFIX_INSTOCK, stockQtys.getAvailable());
			p.hincrBy(curRow.getLocationId(), curRow.getProdCode() + SUFFIX_RESERVED, stockQtys.getReserved());
			p.hincrBy(curRow.getLocationId(), curRow.getProdCode() + SUFFIX_OVERSELL, stockQtys.getOversell());
		}
		p.sync();
	}

	public void updateStockLevelData(final Jedis jedis, final UpdateStockLevelRequest stockLevelReq)
	{
		final Pipeline p = jedis.pipelined();
		final Iterator<StockRow<StockQuantities>> iter = stockLevelReq.getStockRowIterator();
		while (iter.hasNext())
		{
			final StockRow<StockQuantities> curRow = iter.next();
			final StockQuantities stockQtys = curRow.getValue();
			p.hset(curRow.getLocationId(), curRow.getProdCode() + SUFFIX_INSTOCK, String.valueOf(stockQtys.getAvailable()));
			//p.hset(curRow.getLocationId(), curRow.getProdCode() + SUFFIX_RESERVED, String.valueOf(stockQtys.getReserved()));
			//p.hset(curRow.getLocationId(), curRow.getProdCode() + SUFFIX_OVERSELL, String.valueOf(stockQtys.getOversell()));
		}
		p.sync();
	}

	public boolean reserveAllQuantities(final Jedis jedis, final ReserveStockLevelRequest stockLevelReq)
	{
		final List<String> params = stockLevelReq.toScirptParameters();
		final Long res = (Long) jedis.eval(RedisFunctions.allReserveFunc, Collections.EMPTY_LIST, params);
		return res.longValue() == 1;
	}

	public ReserveStockLevelResponse reservePartialQuantities(final Jedis jedis, final ReserveStockLevelRequest stockLevelReq)
	{
		final List<String> params = stockLevelReq.toScirptParameters();
		final List<List<Object>> evalRes = (List<List<Object>>) jedis.eval(RedisFunctions.partialReserveFunc,
				Collections.EMPTY_LIST, params);
		final ReserveStockLevelResponse resp = new ReserveStockLevelResponse();
		for (final List<Object> nextValList : evalRes)
		{
			final String locationId = (String) nextValList.get(0);
			final String prodCode = (String) nextValList.get(1);
			final Long remainQty = (Long) nextValList.get(2);
			resp.addStockLevel(locationId, prodCode, Integer.valueOf(remainQty.intValue()));
		}
		return resp;
	}

	public boolean commitAllQuantities(final Jedis jedis, final CommitStockLevelRequest stockLevelReq)
	{
		final List<String> params = stockLevelReq.toScirptParameters();
		final Long res = (Long) jedis.eval(RedisFunctions.allCommitFunc, Collections.EMPTY_LIST, params);
		return res.longValue() == 1;
	}



	public CommitStockLevelResponse commitPartialQuantities(final Jedis jedis, final CommitStockLevelRequest stockLevelReq)
	{
		final List<String> params = stockLevelReq.toScirptParameters();
		final List<List<Object>> evalRes = (List<List<Object>>) jedis.eval(RedisFunctions.partialCommitFunc,
				Collections.EMPTY_LIST, params);
		final CommitStockLevelResponse resp = new CommitStockLevelResponse();
		for (final List<Object> nextValList : evalRes)
		{
			final String locationId = (String) nextValList.get(0);
			final String prodCode = (String) nextValList.get(1);
			final Long remainQty = (Long) nextValList.get(2);
			resp.addStockLevel(locationId, prodCode, Integer.valueOf(remainQty.intValue()));
		}
		return resp;
	}



	public void rollbackQuantities(final Jedis jedis, final AbstractStockLevelRequest stockLevelReq)
	{
		final List<String> params = stockLevelReq.toScirptParameters();
		jedis.eval(RedisFunctions.allRollbackFunc, Collections.EMPTY_LIST, params);
	}


	public GetQuantityResponse getInstockQuantities(final Jedis jedis, final LocationProducts locProds)
	{
		return this.getSingleQuantities(jedis, locProds, SUFFIX_INSTOCK);
	}

	public int getInstockQuantity(final JedisCommands jedis, final String locationId, final String prodCode)
	{
		final String res = jedis.hget(locationId, prodCode + SUFFIX_INSTOCK);
		return getValue(res);
	}


	public GetQuantityResponse getReservedQuantities(final Jedis jedis, final LocationProducts locProds)
	{
		return this.getSingleQuantities(jedis, locProds, SUFFIX_RESERVED);
	}

	public int getReservedQuantity(final JedisCommands jedis, final String locationId, final String prodCode)
	{
		final String res = jedis.hget(locationId, prodCode + SUFFIX_RESERVED);
		return getValue(res);
	}

	public GetQuantityResponse getAvailableQuantities(final Jedis jedis, final LocationProducts locProds)
	{
		final GetQuantityResponse resp = new GetQuantityResponse();
		final Map<String, Map<String, Response<String>[]>> tmp = new HashMap<String, Map<String, Response<String>[]>>();

		final Pipeline p = jedis.pipelined();
		for (final String nextLoc : locProds.getAllLocations())
		{
			final Set<String> products = locProds.getProductsForLocation(nextLoc);
			for (final String nextProdCode : products)
			{
				final Response<String> instockScore = p.hget(nextLoc, nextProdCode + SUFFIX_INSTOCK);
				final Response<String> reservedScore = p.hget(nextLoc, nextProdCode + SUFFIX_RESERVED);
				final Response<String> oversellScore = p.hget(nextLoc, nextProdCode + SUFFIX_OVERSELL);
				final Response<String>[] arr = new Response[3];
				arr[0] = instockScore;
				arr[1] = reservedScore;
				arr[2] = oversellScore;
				Map<String, Response<String>[]> prodScores = tmp.get(nextLoc);
				if (prodScores == null)
				{
					prodScores = new HashMap<String, Response<String>[]>();
					tmp.put(nextLoc, prodScores);
				}
				prodScores.put(nextProdCode, arr);
			}
		}
		p.sync();

		for (final String nextLoc : tmp.keySet())
		{
			final Map<String, Response<String>[]> srcProdScores = tmp.get(nextLoc);

			for (final String nextProd : srcProdScores.keySet())
			{
				final Response<String>[] nextScores = srcProdScores.get(nextProd);
				final String instockVal = nextScores[0].get();
				final String reservedVal = nextScores[1].get();
				final String oversellVal = nextScores[2].get();
				resp.addStockLevel(nextLoc, nextProd,
						Integer.valueOf(getValue(instockVal) + getValue(oversellVal) - getValue(reservedVal)));
			}
		}
		tmp.clear();
		return resp;
	}



	public MultiGetQuantityResponse getAllQuantities(final Jedis jedis, final LocationProducts locProds)
	{
		final MultiGetQuantityResponse resp = new MultiGetQuantityResponse();
		final Map<String, Map<String, Response<String>[]>> tmp = new HashMap<String, Map<String, Response<String>[]>>();

		final Pipeline p = jedis.pipelined();
		for (final String nextLoc : locProds.getAllLocations())
		{
			final Set<String> products = locProds.getProductsForLocation(nextLoc);
			for (final String nextProdCode : products)
			{
				final Response<String> instockScore = p.hget(nextLoc, nextProdCode + SUFFIX_INSTOCK);
				final Response<String> reservedScore = p.hget(nextLoc, nextProdCode + SUFFIX_RESERVED);
				final Response<String> oversellScore = p.hget(nextLoc, nextProdCode + SUFFIX_OVERSELL);
				final Response<String>[] arr = new Response[3];
				arr[0] = instockScore;
				arr[1] = reservedScore;
				arr[2] = oversellScore;
				Map<String, Response<String>[]> prodScores = tmp.get(nextLoc);
				if (prodScores == null)
				{
					prodScores = new HashMap<String, Response<String>[]>();
					tmp.put(nextLoc, prodScores);
				}
				prodScores.put(nextProdCode, arr);
			}
		}
		p.sync();

		for (final String nextLoc : tmp.keySet())
		{
			final Map<String, Response<String>[]> srcProdScores = tmp.get(nextLoc);

			for (final String nextProd : srcProdScores.keySet())
			{
				final Response<String>[] nextScores = srcProdScores.get(nextProd);
				final String instockVal = nextScores[0].get();
				final String reservedVal = nextScores[1].get();
				final String oversellVal = nextScores[2].get();
				resp.addStockLevel(nextLoc, nextProd, new int[]
				{ getValue(instockVal), getValue(oversellVal), getValue(reservedVal) });
			}
		}
		tmp.clear();
		return resp;
	}



	public int getAvailableQuantity(final JedisCommands jedis, final String locationId, final String prodCode)
	{
		final String instockRes = jedis.hget(locationId, prodCode + SUFFIX_INSTOCK);
		final String reservedRes = jedis.hget(locationId, prodCode + SUFFIX_RESERVED);
		final String oversellRes = jedis.hget(locationId, prodCode + SUFFIX_OVERSELL);
		return getValue(instockRes) + getValue(oversellRes) - getValue(reservedRes);
	}

	public int[] getAllQuantity(final JedisCommands jedis, final String locationId, final String prodCode)
	{
		final String instockRes = jedis.hget(locationId, prodCode + SUFFIX_INSTOCK);
		final String reservedRes = jedis.hget(locationId, prodCode + SUFFIX_RESERVED);
		final String oversellRes = jedis.hget(locationId, prodCode + SUFFIX_OVERSELL);
		return new int[]
		{ getValue(instockRes), getValue(oversellRes), getValue(reservedRes) };
	}


	protected GetQuantityResponse getSingleQuantities(final Jedis jedis, final LocationProducts locProds, final String keySuffix)
	{
		final GetQuantityResponse resp = new GetQuantityResponse();
		final Map<String, Map<String, Response<String>>> tmp = new HashMap<String, Map<String, Response<String>>>();

		final Pipeline p = jedis.pipelined();
		for (final String nextLoc : locProds.getAllLocations())
		{
			final Set<String> products = locProds.getProductsForLocation(nextLoc);
			for (final String nextProdCode : products)
			{
				final Response<String> score = p.hget(nextLoc, nextProdCode + keySuffix);
				Map<String, Response<String>> prodScores = tmp.get(nextLoc);
				if (prodScores == null)
				{
					prodScores = new HashMap<String, Response<String>>();
					tmp.put(nextLoc, prodScores);
				}
				prodScores.put(nextProdCode, score);
			}
		}
		p.sync();

		for (final String nextLoc : tmp.keySet())
		{
			final Map<String, Response<String>> srcProdScores = tmp.get(nextLoc);

			for (final String nextProd : srcProdScores.keySet())
			{
				final Response<String> nextScore = srcProdScores.get(nextProd);
				resp.addStockLevel(nextLoc, nextProd, Integer.valueOf(getValue(nextScore.get())));
			}
		}
		tmp.clear();
		return resp;
	}


	private static int getValue(final String value)
	{
		if (value == null)
		{
			return 0;
		}
		else
		{
			return Integer.parseInt(value);
		}
	}

}
