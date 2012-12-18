package com.splout.db.qnode.rest;

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

import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.splout.db.qnode.IQNodeHandler;

@SuppressWarnings("serial")
public class BaseServlet extends HttpServlet {

	protected final static Log log = LogFactory.getLog(BaseServlet.class);
	protected final transient IQNodeHandler qNodeHandler;

	public BaseServlet(IQNodeHandler qNodeHandler) {
		this.qNodeHandler = qNodeHandler;
	}

}
