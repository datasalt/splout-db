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

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.splout.db.thrift.DNodeService;

/**
 * A Thrift client interface for the {@link DNode} service.
 */
public class DNodeClient {
	
	/**
	 * Get a Thrift client given an address (host:port) 
	 */
	public static DNodeService.Client get(String hostPort) throws TTransportException {
		int separator = hostPort.lastIndexOf(":");
		String host = hostPort.substring(0, separator);
		int port = Integer.parseInt(hostPort.substring(separator + 1, hostPort.length()));
		return get(host, port);
	}

	/**
	 * Get a Thrift client given a host and a port
	 */
	public static DNodeService.Client get(String host, int port) throws TTransportException {
		TTransport transport = new TFramedTransport(new TSocket(host, port));
		TProtocol protocol = new TBinaryProtocol(transport);
		DNodeService.Client client = new DNodeService.Client(protocol);
		transport.open();
		return client;		
	}
	
	/**
	 * It is not trivial to properly close a Thrift client. This method encapsulates this.
	 */
	public static void close(DNodeService.Client client) {
		client.getOutputProtocol().getTransport().close();
	}
}