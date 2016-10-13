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
public class StockRow<T>
{
	private String locationId;
	private String prodCode;
	private T value;

	/**
	 *
	 */
	public StockRow(final String locationId, final String prodCode, final T value)
	{
		this.locationId = locationId;
		this.prodCode = prodCode;
		this.value = value;
	}

	/**
	 * @return the locationId
	 */
	public String getLocationId()
	{
		return locationId;
	}

	/**
	 * @param locationId
	 *           the locationId to set
	 */
	public void setLocationId(final String locationId)
	{
		this.locationId = locationId;
	}

	/**
	 * @return the prodCode
	 */
	public String getProdCode()
	{
		return prodCode;
	}

	/**
	 * @param prodCode
	 *           the prodCode to set
	 */
	public void setProdCode(final String prodCode)
	{
		this.prodCode = prodCode;
	}

	/**
	 * @return the value
	 */
	public T getValue()
	{
		return value;
	}

	/**
	 * @param value
	 *           the value to set
	 */
	public void setValue(final T value)
	{
		this.value = value;
	}


}
