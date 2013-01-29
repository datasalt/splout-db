package com.splout.db.qnode;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.splout.db.common.SploutConfiguration;
import com.splout.db.common.Tablespace;
import com.splout.db.dnode.beans.DNodeSystemStatus;
import com.splout.db.qnode.beans.DeployInfo;
import com.splout.db.qnode.beans.DeployRequest;
import com.splout.db.qnode.beans.QNodeStatus;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.qnode.beans.StatusMessage;
import com.splout.db.qnode.beans.SwitchVersionRequest;

/**
 * Use this interface for implementing the business logic of the {@link QNode} service. One may want to do this for implementing unit tests and validating that the QNode receives requests.
 */
public interface IQNodeHandler {

	public void init(SploutConfiguration config) throws Exception;
	public void close() throws Exception;
	
	public QueryStatus query(String tablespace, String key, String sql, String partition) throws Exception;
	public ArrayList<QueryStatus> multiQuery(String tablespace, List<String> keyMins, List<String> keyMaxs, String sql) throws Exception;
	public DeployInfo deploy(List<DeployRequest> deployReq) throws Exception;
	public DeployInfo createTablespace(DeployRequest deployReq) throws Exception;
	public StatusMessage rollback(List<SwitchVersionRequest> rollbackRequest) throws Exception;
	public QNodeStatus overview() throws Exception;
	public List<String> getDNodeList() throws Exception;
	public Set<String> tablespaces() throws Exception;
	public Map<Long, Tablespace> allTablespaceVersions(String tablespace) throws Exception;
	public DNodeSystemStatus dnodeStatus(String dNode) throws Exception;
	public Tablespace tablespace(String tablespace) throws Exception;
}
