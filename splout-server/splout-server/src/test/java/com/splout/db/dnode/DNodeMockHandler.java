package com.splout.db.dnode;

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

import java.util.List;

import com.splout.db.common.SploutConfiguration;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DeployAction;
import com.splout.db.thrift.RollbackAction;
import com.splout.db.thrift.TablespaceVersion;

/**
 * A skeleton for DNode Mocks so that test cases only override the methods needed.
 * When changing IDNodeHandler, most of the cases it will be only neded to modify this class.
 */
public class DNodeMockHandler implements IDNodeHandler {

	@Override
  public void init(SploutConfiguration config) throws Exception {
  }
	@Override
  public void giveGreenLigth() {
  }
	@Override
  public String sqlQuery(String tablespace, long version, int partition, String query)
      throws DNodeException {
	  return null;
  }
	@Override
  public String deploy(List<DeployAction> deployActions, long version) throws DNodeException {
	  return null;
  }
	@Override
  public String rollback(List<RollbackAction> rollbackActions, String ignoreMe) throws DNodeException {
	  return null;
  }
	@Override
  public String status() throws DNodeException {
	  return null;
  }
	@Override
  public String abortDeploy(long version) throws DNodeException {
	  return null;
  }
	@Override
  public String deleteOldVersions(List<TablespaceVersion> versions) throws DNodeException {
	  return null;
  }
	@Override
  public String testCommand(String command) throws DNodeException {
	  return null;
  }
	@Override
  public void stop() throws Exception {
  }
}
