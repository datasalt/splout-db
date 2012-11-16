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

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

/**
 * A HttpServletFilter to set the header "Cache-Control" to "No-cache". 
 * That is important for Splout, as we don't want performed requests
 * to database to be cached by browsers. 
 */
public class NoCacheFilter implements Filter {

	public NoCacheFilter() {
	}

	@Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

	@Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
		HttpServletResponse httpResponse = (HttpServletResponse) response;

		if(httpResponse != null) {
			httpResponse.setHeader("Cache-Control", "No-cache");
		}
		//httpResponse.setDateHeader(“Expires”, 0);
		//httpResponse.setHeader(“Pragma”, “No-cache”);
		
		chain.doFilter(request, response);
  }

	@Override
  public void destroy() {
  }

}
