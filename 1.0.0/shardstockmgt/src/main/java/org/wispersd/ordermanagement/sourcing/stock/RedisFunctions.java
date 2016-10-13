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

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 *
 */
public class RedisFunctions
{
	private static final Logger logger = LoggerFactory.getLogger(RedisFunctions.class);

	public static String allReserveFunc = null;
	public static String partialReserveFunc = null;
	public static String allCommitFunc = null;
	public static String partialCommitFunc = null;
	public static String allRollbackFunc = null;

	static
	{
		allReserveFunc = loadScript("allreserve.lua");
		partialReserveFunc = loadScript("partialreserve.lua");
		allCommitFunc = loadScript("allcommit.lua");
		partialCommitFunc = loadScript("partialcommit.lua");
		allRollbackFunc = loadScript("allrollback.lua");
	}


	public static String loadScript(final String scriptName)
	{
		final StringBuilder sb = new StringBuilder();
		try
		{
			final BufferedReader br = new BufferedReader(new InputStreamReader(RedisFunctions.class.getClassLoader()
					.getResourceAsStream(scriptName)));
			String nextLine = br.readLine();
			while (nextLine != null && (!"".equals(nextLine.trim())))
			{
				sb.append(nextLine).append("\n");
				nextLine = br.readLine();
			}
		}
		catch (final Exception ex)
		{
			logger.error("error loading script " + scriptName, ex);
		}
		return sb.toString();
	}

}
