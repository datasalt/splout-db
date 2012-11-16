/**
 * This package contains all the code related to the {@link com.splout.db.dnode.DNode} service of Splout. In the Splout
 * architecture, the DNode is the responsible for storing SQL databases in disk. It communicates
 * via Thrift with the QNode. The QNode sends queries to DNodes and they respond to them.
 * <p>
 * The main class for DNode is {@link com.splout.db.dnode.DNode}. The main business logic is in {@link com.splout.db.dnode.DNodeHandler}.
 * <p>
 * The {@link com.splout.db.common.SploutConfiguration} properties related to the DNode are in {@link com.splout.db.dnode.DNodeProperties}.
 */
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
