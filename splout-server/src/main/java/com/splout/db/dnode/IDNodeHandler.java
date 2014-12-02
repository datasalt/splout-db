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

import java.nio.ByteBuffer;
import java.util.List;

import com.splout.db.common.SploutConfiguration;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DeployAction;
import com.splout.db.thrift.RollbackAction;
import com.splout.db.thrift.TablespaceVersion;

/**
 * Use this interface for implementing the business logic of the {@link DNode}
 * service. One may want to do this for implementing unit tests and validating
 * that the DNode receives requests.
 */
public interface IDNodeHandler {

  public void init(SploutConfiguration config) throws Exception;

  /**
   * Method called when {@link #init(SploutConfiguration)} has been performed
   * and the service is ready to accept request. Typically, this method will
   * register the service as ready in the cluster.
   */
  public void giveGreenLigth();

  public ByteBuffer binarySqlQuery(String tablespace, long version, int partition, String query, int cursorId) throws DNodeException;

  public String sqlQuery(String tablespace, long version, int partition, String query)
      throws DNodeException;

  public String deploy(final List<DeployAction> deployActions, final long version) throws DNodeException;

  public String rollback(List<RollbackAction> rollbackActions, String ignoreMe) throws DNodeException;

  public String status() throws DNodeException;

  public String abortDeploy(long version) throws DNodeException;

  public String deleteOldVersions(List<TablespaceVersion> versions) throws DNodeException;

  public String testCommand(String command) throws DNodeException;

  public void stop() throws Exception;
}
