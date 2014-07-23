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

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.resource.Resource;
import org.mortbay.resource.ResourceCollection;

import com.splout.db.common.SploutConfiguration;
import com.splout.db.qnode.rest.AdminServlet;
import com.splout.db.qnode.rest.DeployRollbackServlet;
import com.splout.db.qnode.rest.QueryServlet;
import com.splout.db.qnode.rest.RewriteRuleHandler;

/**
 * Like the {@link com.splout.db.dnode.DNode}, this class is only the skeleton of the QNode service. It handles the HTTP
 * requests and delegates them to the business logic in {@link IQNodeHandler}.
 * <p>
 * The HTTP handling is implemented using Jersey API in {@link RESTAPI}.
 * 
 * @see QNodeHandler
 */
public class QNode {

	private IQNodeHandler handler;
	private String address;
	private Server server;

	public void start(SploutConfiguration config, IQNodeHandler handler) throws Exception {
		this.handler = handler;
		boolean init = false;
		int retries = 0;

		handler.init(config);

		do {
			try {
				server = new Server(config.getInt(QNodeProperties.PORT));
        server.setAttribute("headerBufferSize", 65535);

        RewriteRuleHandler rewrite = new RewriteRuleHandler();
				
				WebAppContext context = new WebAppContext();
				context.setContextPath("/");
				DefaultServlet defaultServlet = new DefaultServlet();
				
				context.addServlet(new ServletHolder(new QueryServlet(handler)), "/api/query");
				context.addServlet(new ServletHolder(new AdminServlet(handler)), "/api/admin");
				context.addServlet(new ServletHolder(new DeployRollbackServlet(handler)), "/api/deploy");
				
				context.addServlet(new ServletHolder(defaultServlet), "/panel/*");
        context.addServlet(new ServletHolder(new com.yammer.metrics.reporting.AdminServlet()), "/metrics/*");
				
				// No cache header in all responses... otherwise some browsers
				// can decide to cache some requests and they shouldn't
				context.addFilter(NoCacheFilter.class, "/*", Handler.DEFAULT);
				
				ResourceCollection resources = new ResourceCollection(new String[] { Resource
				    .newClassPathResource("panel").toString() });

				context.setBaseResource(resources);

				rewrite.setHandler(context);
				server.setHandler(rewrite);
				server.start();

				address = "http://" + config.getString(QNodeProperties.HOST) + ":"
				    + config.getInt(QNodeProperties.PORT);

				init = true;
			} catch(java.net.BindException e) {
				if(!config.getBoolean(QNodeProperties.PORT_AUTOINCREMENT)) {
					throw e;
				}
				config.setProperty(QNodeProperties.PORT, config.getInt(QNodeProperties.PORT) + 1);
				retries++;
			}
		} while(!init && retries < 100);
	}

	public String getAddress() {
		return address;
	}

	public IQNodeHandler getHandler() {
		return handler;
	}

	public void close() throws Exception {
		handler.close();
		server.stop();
	}

	public static void main(String[] args) throws Exception {
		QNode qnode = new QNode();
		SploutConfiguration config = SploutConfiguration.get();
		qnode.start(config, new QNodeHandler());
		while(true) {
			Thread.sleep(100);
		}
	}
}
