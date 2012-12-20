package com.splout.db.examples;

/*
 * #%L
 * Splout SQL Hadoop library
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.SploutClient;

/**
 * Servlet that proxies AJAX request to an arbitrary QNode for implementing the frontend of the Wikipedia pagecounts
 * example.
 */
@SuppressWarnings("serial")
public class PageCountsServlet extends HttpServlet {

	private final static Log log = LogFactory.getLog(PageCountsServlet.class);
	private SploutClient client;

	public PageCountsServlet(SploutClient client) {
		this.client = client;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
	    IOException {

		String sql = req.getParameter("sql");
		String key = req.getParameter("key");

		log.info("query, key[" + key + "] sql[" + sql + "]");
		try {
			resp.getWriter().write(JSONSerDe.ser(client.query("pagecounts", key, sql, null)));
		} catch(JSONSerDeException e) {
			throw new IOException(e);
		}

	}
}
