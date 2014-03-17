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

@SuppressWarnings("serial")
public class AdminServlet extends BaseServlet {

	public final static String ACTION_DNODE_STATUS = "dnodestatus";
	public final static String ACTION_ALL_TABLESPACE_VERSIONS = "alltablespaceversions";
	public final static String ACTION_TABLESPACE_INFO = "tablespaceinfo";
	public final static String ACTION_TABLESPACES = "tablespaces";
	public final static String ACTION_DNODE_LIST = "dnodelist";
	public final static String ACTION_OVERVIEW = "overview";
	public final static String ACTION_DEPLOYMENTS_STATUS = "deploymentsstatus"; 
	public final static String ACTION_CLEAN_OLD_VERSIONS = "cleanoldversions";
	
	public AdminServlet(IQNodeHandler qNodeHandler) {
	  super(qNodeHandler);
  }

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
	    IOException {

		String action = req.getParameter("action");
		
		resp.setHeader("content-type", "application/json;charset=UTF-8");
		resp.setCharacterEncoding("UTF-8");
		
		String response = null;
		try {
			if(action.equals(ACTION_DNODE_STATUS)) {
				String dnode = req.getParameter("dnode");
				response = JSONSerDe.ser(qNodeHandler.dnodeStatus(dnode));
			} else if(action.equals(ACTION_ALL_TABLESPACE_VERSIONS)) {
				String tablespace = req.getParameter("tablespace");
				response = JSONSerDe.ser(qNodeHandler.allTablespaceVersions(tablespace));
			} else if(action.equals(ACTION_TABLESPACE_INFO)) {
				String tablespace = req.getParameter("tablespace");
				response = JSONSerDe.ser(qNodeHandler.tablespace(tablespace));
			} else if(action.equals(ACTION_TABLESPACES)) {
				response = JSONSerDe.ser(qNodeHandler.tablespaces());				
			} else if(action.equals(ACTION_DNODE_LIST)) {
				response = JSONSerDe.ser(qNodeHandler.getDNodeList());								
			} else if(action.equals(ACTION_OVERVIEW)) {
				response = JSONSerDe.ser(qNodeHandler.overview());	
			} else if(action.equals(ACTION_DEPLOYMENTS_STATUS)) {
				response = JSONSerDe.ser(qNodeHandler.deploymentsStatus());
			} else {
				throw new ServletException("Unknown action: " + action);
			}
			resp.getWriter().append(response);
		} catch(Exception e) {
			log.error(e);
			throw new ServletException(e);
		}
	}
}
