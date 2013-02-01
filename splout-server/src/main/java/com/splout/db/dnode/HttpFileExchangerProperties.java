package com.splout.db.dnode;

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
