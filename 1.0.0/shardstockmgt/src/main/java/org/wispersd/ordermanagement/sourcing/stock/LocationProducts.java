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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 *
 */
public class LocationProducts
{
	private final Map<String, Set<String>> params = new HashMap<String, Set<String>>();

	public LocationProducts()
	{
	}

	public LocationProducts(final Map<String, Set<String>> p)
	{
		this.params.putAll(p);
	}


	public Set<String> getAllLocations()
	{
		return params.keySet();
	}

	public Set<String> getProductsForLocation(final String locationId)
	{
		return params.get(locationId);
	}

	public void addProductForLocation(final String locationId, final String prodCode)
	{
		Set<String> products = params.get(locationId);
		if (products == null)
		{
			products = new HashSet<String>();
			params.put(locationId, products);
		}
		products.add(prodCode);
	}

	public void addProductsForLocation(final String locationId, final Set<String> prodCodes)
	{
		Set<String> products = params.get(locationId);
		if (products == null)
		{
			products = new HashSet<String>();
			params.put(locationId, products);
		}
		products.addAll(prodCodes);
	}


	@Override
	public String toString()
	{
		return "LocationProducts [params=" + params + "]";
	}
}
