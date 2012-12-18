package com.splout.db.qnode.rest;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.splout.db.common.JSONSerDe;
import com.splout.db.qnode.IQNodeHandler;
import com.splout.db.qnode.beans.QueryStatus;

@SuppressWarnings("serial")
public class QueryServlet extends BaseServlet {

	public QueryServlet(IQNodeHandler qNodeHandler) {
	  super(qNodeHandler);
  }

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
	    IOException {

		String[] keys = req.getParameterValues("key");
		String tablespace = req.getParameter("tablespace");
		String sql = req.getParameter("sql");
		String callback = req.getParameter("callback");

		resp.setHeader("content-type", "application/json;charset=UTF-8");

		String key = "";
		for(String strKey : keys) {
			key += strKey;
		}
		try {
			QueryStatus st = qNodeHandler.query(tablespace, key, sql);
			log.info(Thread.currentThread().getName() + ": Query request received, tablespace[" + tablespace
			    + "], key[" + key + "], sql[" + sql + "] time [" + st.getMillis() + "]");
			String response;
			response = JSONSerDe.ser(st);
			if(callback != null) {
				response = callback + "(" + response + ")";
			}
			resp.getWriter().append(response);
		} catch(Exception e) {
			log.error(e);
			throw new ServletException(e);
		}
	}
}
