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
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.type.TypeReference;

import com.splout.db.common.JSONSerDe;
import com.splout.db.qnode.IQNodeHandler;
import com.splout.db.qnode.beans.DeployRequest;
import com.splout.db.qnode.beans.SwitchVersionRequest;

@SuppressWarnings("serial")
public class DeployRollbackServlet extends BaseServlet {

	public final static String ACTION_DEPLOY = "deploy";
	public final static String ACTION_ROLLBACK = "rollback";

	public DeployRollbackServlet(IQNodeHandler qNodeHandler) {
		super(qNodeHandler);
	}

	public final static TypeReference<ArrayList<SwitchVersionRequest>> ROLLBACK_REQ_REF = new TypeReference<ArrayList<SwitchVersionRequest>>() {
	};
	public final static TypeReference<ArrayList<DeployRequest>> DEPLOY_REQ_REF = new TypeReference<ArrayList<DeployRequest>>() {
	};
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
	    IOException {

		StringBuffer postBody = new StringBuffer();
		String line = null;

		BufferedReader reader = req.getReader();
		while((line = reader.readLine()) != null) {
			postBody.append(line);
		}
		
		resp.setHeader("content-type", "application/json;charset=UTF-8");
		resp.setCharacterEncoding("UTF-8");

		String action = req.getParameter("action");
		String response = null;

		try {
			if(action.equals(ACTION_DEPLOY)) {
				List<DeployRequest> deployReq = JSONSerDe.deSer(postBody.toString(), DEPLOY_REQ_REF);
				// List of DeployRequest
				log.info(Thread.currentThread().getName() + ": Deploy request received [" + deployReq + "]");
				for(DeployRequest request : deployReq) {
					if(request.getReplicationMap() == null || request.getReplicationMap().size() < 1) {
						throw new IllegalArgumentException("Invalid deploy request with empty replication map ["
						    + request + "]");
					}
					if(request.getPartitionMap().size() != request.getReplicationMap().size()) {
						throw new IllegalArgumentException(
						    "Invalid deploy request with non-coherent replication / partition maps [" + request + "]");
					}
				}
				response = JSONSerDe.ser(qNodeHandler.deploy(deployReq));
			} else if(action.equals(ACTION_ROLLBACK)) {
				ArrayList<SwitchVersionRequest> rReq = JSONSerDe.deSer(postBody.toString(), ROLLBACK_REQ_REF);
				// List of RollbackRequest
				log.info(Thread.currentThread().getName() + ": Rollback request received [" + rReq + "]");
				response = JSONSerDe.ser(qNodeHandler.rollback(rReq));
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
