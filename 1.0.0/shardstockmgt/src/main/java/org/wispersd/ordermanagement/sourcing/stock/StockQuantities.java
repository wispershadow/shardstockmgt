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

/**
 *
 */
public class StockQuantities
{
	private int available;
	private int reserved;
	private int oversell;

	/**
	 * @return the available
	 */
	public int getAvailable()
	{
		return available;
	}

	/**
	 * @param available
	 *           the available to set
	 */
	public void setAvailable(final int available)
	{
		this.available = available;
	}

	/**
	 * @return the reserved
	 */
	public int getReserved()
	{
		return reserved;
	}

	/**
	 * @param reserved
	 *           the reserved to set
	 */
	public void setReserved(final int reserved)
	{
		this.reserved = reserved;
	}

	/**
	 * @return the oversell
	 */
	public int getOversell()
	{
		return oversell;
	}

	/**
	 * @param oversell
	 *           the oversell to set
	 */
	public void setOversell(final int oversell)
	{
		this.oversell = oversell;
	}

	@Override
	public String toString()
	{
		return "StockQuantities [available=" + available + ", reserved=" + reserved + ", oversell=" + oversell + "]";
	}
}
