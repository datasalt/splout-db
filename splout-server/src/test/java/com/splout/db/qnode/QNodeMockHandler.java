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
import com.splout.db.qnode.beans.DeploymentsStatus;
import com.splout.db.qnode.beans.QNodeStatus;
import com.splout.db.qnode.beans.QueryStatus;
import com.splout.db.qnode.beans.StatusMessage;
import com.splout.db.qnode.beans.SwitchVersionRequest;

/**
 * A skeleton for QNode Mocks so that test cases only override the methods needed.
 * When changing IQNodeHandler, most of the cases it will be only neded to modify this class.
 */
public class QNodeMockHandler implements IQNodeHandler {

	@Override
  public void init(SploutConfiguration config) throws Exception {
  }
	@Override
  public void close() throws Exception {
  }
	@Override
  public QueryStatus query(String tablespace, String key, String sql, String partition, Integer cursorId) throws Exception {
	  return null;
  }
	@Override
  public ArrayList<QueryStatus> multiQuery(String tablespace, List<String> keyMins,
      List<String> keyMaxs, String sql) throws Exception {
	  return null;
  }
	@Override
  public DeployInfo deploy(List<DeployRequest> deployReq) throws Exception {
	  return null;
  }
	@Override
  public StatusMessage rollback(List<SwitchVersionRequest> rollbackRequest) throws Exception {
	  return null;
  }
	@Override
  public QNodeStatus overview() throws Exception {
	  return null;
  }
	@Override
  public List<String> getDNodeList() throws Exception {
	  return null;
  }
	@Override
  public Set<String> tablespaces() throws Exception {
	  return null;
  }
	@Override
  public Map<Long, Tablespace> allTablespaceVersions(String tablespace) throws Exception {
	  return null;
  }
	@Override
  public DNodeSystemStatus dnodeStatus(String dNode) throws Exception {
	  return null;
  }
	@Override
  public Tablespace tablespace(String tablespace) throws Exception {
	  return null;
  }
	@Override
  public DeploymentsStatus deploymentsStatus() throws Exception {
	  return null;
  }
	@Override
  public StatusMessage cleanOldVersions() throws Exception {
	  return null;
  }
}
