package com.splout.db.dnode;

public class HttpFileExchangerProperties {

	/*
	 * The number of serving threads of the HTTP server (maximum files that can be transferred in parallel)
	 */
	public final static String HTTP_THREADS = "http.exchanger.threads";
	/*
	 * The allowed maximum backlog for the HTTP server  
	 */	
	public final static String HTTP_BACKLOG = "http.exchanger.backlog";
	/*
	 * The port where the HTTP file exchanger will bind to 
	 */
	public final static String HTTP_PORT = "http.exchanger.port";
}
