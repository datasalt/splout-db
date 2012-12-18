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
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.handler.HandlerWrapper;

/**
 * We don't use Jetty's rewrite handler because it has a bug which is not fixed until version 8.
 */
public class RewriteRuleHandler extends HandlerWrapper {

	Pattern tablespaceInfo = Pattern.compile("/api/tablespace/([^/]*)(/)?");
	Pattern tablespaceVersions = Pattern.compile("/api/tablespace/([^/]*)/versions(/)?");
	Pattern dnodeInfo = Pattern.compile("/api/dnode/([^/]*)(/)?");
	Pattern query = Pattern.compile("/api/query/([^/]*)(/)?");

	/**
	 * An implementation of HttpServletRequestWrapper which we can use to add more parameters if we want.
	 */
	public static class MutableHttpRequest extends HttpServletRequestWrapper {

		private HashMap<String, String> params = new HashMap<String, String>();

		public MutableHttpRequest(HttpServletRequest request) {
			super(request);
		}

		public String getParameter(String name) {
			// if we added one, return that one
			if(params.get(name) != null) {
				return params.get(name);
			}
			// otherwise return what's in the original request
			HttpServletRequest req = (HttpServletRequest) super.getRequest();
			return req.getParameter(name);
		}

		public void addParameter(String name, String value) {
			params.put(name, value);
		}
	}

	@Override
	public void handle(String target, HttpServletRequest request, HttpServletResponse response,
	    int dispatch) throws IOException, ServletException {

		/**
		 * {@link QueryServlet} rewrite rules
		 */
		Matcher m = query.matcher(target);
		if(m.matches()) {
	  	String tablespace = m.group(1);
			MutableHttpRequest req = new MutableHttpRequest(request);
			req.addParameter("tablespace", tablespace);
			super.handle("/api/query", req, response, dispatch);						
  	}

		/**
		 * {@link AdminServlet} rewrite rules 
		 */
		
		if(target.startsWith("/api/overview")) {
			MutableHttpRequest req = new MutableHttpRequest(request);
			req.addParameter("action", AdminServlet.ACTION_OVERVIEW);
			super.handle("/api/admin", req, response, dispatch);
		} 
		
		if(target.startsWith("/api/dnodelist")) {
			MutableHttpRequest req = new MutableHttpRequest(request);
			req.addParameter("action", AdminServlet.ACTION_DNODE_LIST);
			super.handle("/api/admin", req, response, dispatch);
		}
		
		if(target.startsWith("/api/tablespaces")) {
			MutableHttpRequest req = new MutableHttpRequest(request);
			req.addParameter("action", AdminServlet.ACTION_TABLESPACES);
			super.handle("/api/admin", req, response, dispatch);
		}
		
		m = tablespaceInfo.matcher(target);
		if(m.matches()) {
	  	String tablespace = m.group(1);
			MutableHttpRequest req = new MutableHttpRequest(request);
			req.addParameter("action", AdminServlet.ACTION_TABLESPACE_INFO);
			req.addParameter("tablespace", tablespace);
			super.handle("/api/admin", req, response, dispatch);						
  	} 
		
		m = dnodeInfo.matcher(target);
		if(m.matches()) {
	  	String dnode = m.group(1);
			MutableHttpRequest req = new MutableHttpRequest(request);
			req.addParameter("action", AdminServlet.ACTION_DNODE_STATUS);
			req.addParameter("dnode", dnode);
			super.handle("/api/admin", req, response, dispatch);						
		} 
		
		m = tablespaceVersions.matcher(target);
		if(m.matches()) {
	  	String tablespace = m.group(1);
			MutableHttpRequest req = new MutableHttpRequest(request);
			req.addParameter("action", AdminServlet.ACTION_ALL_TABLESPACE_VERSIONS);
			req.addParameter("tablespace", tablespace);
			super.handle("/api/admin", req, response, dispatch);						
		} 
		
		/**
		 * {@link DeployRollbackServlet} rewrite rules 
		 */
		if(target.startsWith("/api/deploy")) {
			MutableHttpRequest req = new MutableHttpRequest(request);
			req.addParameter("action", DeployRollbackServlet.ACTION_DEPLOY);
			super.handle("/api/deploy", req, response, dispatch);			
		}
		
		if(target.startsWith("/api/rollback")) {
			MutableHttpRequest req = new MutableHttpRequest(request);
			req.addParameter("action", DeployRollbackServlet.ACTION_ROLLBACK);
			super.handle("/api/deploy", req, response, dispatch);						
	  }
		
		super.handle(target, request, response, dispatch);
	}
}
