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

import com.splout.db.common.JSONSerDe;
import com.splout.db.qnode.IQNodeHandler;
import com.splout.db.qnode.QNode;
import com.splout.db.qnode.beans.DeployRequest;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.qnode.beans.SwitchVersionRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.type.TypeReference;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.ArrayList;
import java.util.List;

/**
 * Jersey (<a href='http://jersey.java.net/'>http://jersey.java.net/</a>) implementation of a RESTful webservice for
 * {@link QNode}. It delegates the requests to the {@link IQNodeHandler} that is in the context. <br>
 */
@Path("/")
public class RESTAPI {

	@Context
	ResourceConfig rc;

	private final static Log log = LogFactory.getLog(RESTAPI.class);

	@GET
	@Path("/query/{tablespace}")
	@Produces({ "application/json;charset=UTF-8" })
	public String query(@PathParam("tablespace") String tablespace, @QueryParam("key") List<String> keys,
	    @QueryParam("sql") String sql, @QueryParam("callback") String callback) throws Exception {

		// When receiving more than one key we just concatenate them in the same way partition by multiple fields is done
		// Notice that order is important here and it shoulb de consistent with the "partition by" clause, otherwise
		// resultant concatenated key will be different.
		String key = "";
		for(String strKey : keys) {
			key += strKey;
		}
		QueryStatus st = ((IQNodeHandler) rc.getProperties().get("handler")).query(tablespace, key, sql);
		log.info(Thread.currentThread().getName() + ": Query request received, tablespace[" + tablespace
		    + "], key[" + key + "], sql[" + sql + "] time [" + st.getMillis() + "]");
		String resp = JSONSerDe.ser(st);
		// For supporting cross domain requests
		if(callback != null) {
			resp = callback + "(" + resp + ")";
		}
		return resp;
	}

	@GET
	@Path("/multiquery/{tablespace}")
	@Produces({ "application/json;charset=UTF-8" })
	public String multiQuery(@QueryParam("keymins") List<String> keyMins,
	    @QueryParam("keymaxs") List<String> keyMaxs, @PathParam("tablespace") String tablespace,
	    @QueryParam("sql") String sql, @QueryParam("callback") String callback) throws Exception {

		log.info(Thread.currentThread().getName() + ": MultiQuery request received, tablespace["
		    + tablespace + "], keymins[" + keyMins + "], keymaxs[" + keyMaxs + "], sql[" + sql + "]");
		// For supporting cross domain requests
		String resp = JSONSerDe.ser(((IQNodeHandler) rc.getProperties().get("handler")).multiQuery(
		    tablespace, keyMins, keyMaxs, sql));
		if(callback != null) {
			resp = callback + "(" + resp + ")";
		}
		return resp;
	}

	public final static TypeReference<ArrayList<DeployRequest>> DEPLOY_REQ_REF = new TypeReference<ArrayList<DeployRequest>>() {
	};

	@POST
	@Path("/deploy")
	@Produces({ "application/json;charset=UTF-8" })
	public String deploy(String body) throws Exception {

		List<DeployRequest> deployReq = JSONSerDe.deSer(body, DEPLOY_REQ_REF);
		log.info(Thread.currentThread().getName() + ": Deploy request received [" + deployReq + "]"); // List of
		                                                                                              // DeployRequest
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
		return JSONSerDe.ser(((IQNodeHandler) rc.getProperties().get("handler")).deploy(deployReq));
	}

	public final static TypeReference<ArrayList<SwitchVersionRequest>> ROLLBACK_REQ_REF = new TypeReference<ArrayList<SwitchVersionRequest>>() {
	};

	@POST
	@Path("/rollback")
	@Produces({ "application/json;charset=UTF-8" })
	public String rollback(String body) throws Exception {

		ArrayList<SwitchVersionRequest> rReq = JSONSerDe.deSer(body, ROLLBACK_REQ_REF);
		log.info(Thread.currentThread().getName() + ": Rollback request received [" + rReq + "]"); // List of
		                                                                                           // RollbackRequest
		return JSONSerDe.ser(((IQNodeHandler) rc.getProperties().get("handler")).rollback(rReq));
	}

	@GET
	@Path("/overview")
	@Produces({ "application/json;charset=UTF-8" })
	public String overview() throws Exception {

		log.info(Thread.currentThread().getName() + ": Status request received");
		return JSONSerDe.ser(((IQNodeHandler) rc.getProperties().get("handler")).overview());
	}

	@GET
	@Path("/dnodelist")
	@Produces({ "application/json;charset=UTF-8" })
	public String dNodeList() throws Exception {

		log.info(Thread.currentThread().getName() + ": Get DNode List request received");
		return JSONSerDe.ser(((IQNodeHandler) rc.getProperties().get("handler")).getDNodeList());
	}

	@GET
	@Path("/tablespaces")
	@Produces({ "application/json;charset=UTF-8" })
	public String tablespaces() throws Exception {

		return JSONSerDe.ser(((IQNodeHandler) rc.getProperties().get("handler")).tablespaces());
	}

	@GET
	@Path("/tablespace/{tablespace}")
	@Produces({ "application/json;charset=UTF-8" })
	public String tablespace(@PathParam("tablespace") String tablespace) throws Exception {

		return JSONSerDe.ser(((IQNodeHandler) rc.getProperties().get("handler")).tablespace(tablespace));
	}

	@GET
	@Path("/tablespace/{tablespace}/versions")
	@Produces({ "application/json;charset=UTF-8" })
	public String tablespaceVersions(@PathParam("tablespace") String tablespace) throws Exception {

		return JSONSerDe.ser(((IQNodeHandler) rc.getProperties().get("handler"))
		    .allTablespaceVersions(tablespace));
	}

	@GET
	@Path("/dnode/{dnode}/status")
	@Produces({ "application/json;charset=UTF-8" })
	public String dnodeStatus(@PathParam("dnode") String dnode) throws Exception {

		return JSONSerDe.ser(((IQNodeHandler) rc.getProperties().get("handler")).dnodeStatus(dnode));
	}

}
