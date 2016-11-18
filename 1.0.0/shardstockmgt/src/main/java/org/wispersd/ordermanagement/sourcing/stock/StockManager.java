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
public interface StockManager
{
	public void clearAllStockData();

	public void addStockLevelData(final AddStockLevelRequest stockLevelReq);

	public void updateStockLevelData(final UpdateStockLevelRequest stockLevelReq);

	public boolean reserveAllQuantities(final ReserveStockLevelRequest stockLevelReq);

	public ReserveStockLevelResponse reservePartialQuantities(final ReserveStockLevelRequest stockLevelReq);

	public boolean commitAllQuantities(CommitStockLevelRequest stockLevelReq);

	public CommitStockLevelResponse commitPartitalQuantities(final CommitStockLevelRequest stockLevelReq);

	public void rollbackQuantities(final RollbackStockLevelRequest stockLevelReq);

	public GetQuantityResponse getInstockQuantities(LocationProducts locProds);

	public GetQuantityResponse getReservedQuantities(LocationProducts locProds);

	public GetQuantityResponse getAvailableQuantities(LocationProducts locProds);

	public MultiGetQuantityResponse getAllQuantities(LocationProducts locProds);

	public int getInstockQuantity(final String locationId, final String prodCode);

	public int getReservedQuantity(final String locationId, final String prodCode);

	public int getAvailableQuantity(final String locationId, final String prodCode);

	public int[] getAllQuantities(final String locationId, final String prodCode);
}
