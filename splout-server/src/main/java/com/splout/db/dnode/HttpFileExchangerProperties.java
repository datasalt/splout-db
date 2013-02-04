package com.splout.db.dnode;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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

public class HttpFileExchangerProperties {

	/*
	 * The number of serving threads of the HTTP server (maximum files that can be received in parallel)
	 */
	public final static String HTTP_THREADS_SERVER = "http.exchanger.threads.server";
	/*
	 * The number of serving threads of the HTTP client (maximum files that can be sent in parallel)
	 */
	public final static String HTTP_THREADS_CLIENT = "http.exchanger.threads.client";
	/*
	 * The allowed maximum backlog for the HTTP server  
	 */	
	public final static String HTTP_BACKLOG = "http.exchanger.backlog";
	/*
	 * The port where the HTTP file exchanger will bind to 
	 */
	public final static String HTTP_PORT = "http.exchanger.port";
	/*
	 * Whether or not to use auto-increment for the HTTP port in case it is already busy 
	 */
	public final static String HTTP_PORT_AUTO_INCREMENT = "http.exchanger.port.auto.increment";
}
