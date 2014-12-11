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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.splout.db.common.JSONSerDe;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.qnode.IQNodeHandler;
import com.splout.db.qnode.beans.ErrorQueryStatus;
import com.splout.db.qnode.beans.QueryStatus;

@SuppressWarnings("serial")
public class QueryServlet extends BaseServlet {

  public QueryServlet(IQNodeHandler qNodeHandler) {
    super(qNodeHandler);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {

		String tablespace = req.getParameter("tablespace");
		
		StringBuffer postBody = new StringBuffer();
		String line = null;

    BufferedReader reader = req.getReader();
    while ((line = reader.readLine()) != null) {
      postBody.append(line);
    }

    Map<String, Object> params;
    try {
	    params = JSONSerDe.deSer(postBody.toString(), Map.class);
			String[] keys = (String[]) ((ArrayList) params.get("key")).toArray(new String[0]);
			String sql = (String) params.get("sql");
			String callback = (String) params.get("callback");
			String partition = (String) params.get("partition");
			
			handle(req, resp, keys, tablespace, sql, callback, partition);
    } catch(JSONSerDeException e) {
	    throw new IOException(e);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {

    String[] keys = req.getParameterValues("key");
    String tablespace = req.getParameter("tablespace");
    String sql = req.getParameter("sql");
    String callback = req.getParameter("callback");
    String partition = req.getParameter("partition");

    handle(req, resp, keys, tablespace, sql, callback, partition);
  }

  private void handle(HttpServletRequest req, HttpServletResponse resp, String[] keys, String tablespace, String sql, String callback, String partition) throws ServletException,
      IOException {

    resp.setHeader("content-type", "application/json;charset=UTF-8");
    resp.setCharacterEncoding("UTF-8");

    String key = null;
    if (keys != null) {
      key = "";
      for (String strKey : keys) {
        key += strKey;
      }
    }

    try {
      long startTime = System.currentTimeMillis();
      QueryStatus st = qNodeHandler.query(tablespace, key, sql, partition);
      String status = "status[OK]";
      if (st instanceof ErrorQueryStatus) {
        String errMsg = st.getError();
        errMsg = errMsg != null ? errMsg.replace("[", "(").replace("]", ")") : null;
        status = "status[ERROR] errMessage[" + errMsg + "]";
      }
      log.info("Query request received, tablespace[" + tablespace
          + "], key[" + key + "], sql[" + sql + "] time[" + (System.currentTimeMillis() - startTime) + "] " + status);
      String response;
      response = JSONSerDe.ser(st);
      if (callback != null) {
        response = callback + "(" + response + ")";
      }
      resp.getWriter().append(response);
    } catch (Exception e) {
      log.error(e);
      throw new ServletException(e);
    }
  }
}
