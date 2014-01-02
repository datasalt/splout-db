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

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.event.CacheEventListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.splout.db.engine.EngineManager;

/**
 * An EHCache event listener that calls a finalization method in the value of the Cache which is a {@link EngineManager}
 * . We use an expiring cache in {@link DNode} for closing SQL connection pools that have not been used for some time.
 * We want to close the pool when the item expires and that's what this class does.
 */
public class CacheListener implements CacheEventListener, Cloneable {

	private final static Log log = LogFactory.getLog(CacheListener.class);

	/*
	 * Here is where we close the connection pool
	 */
	protected void closeManager(Element paramElement) {
		log.info("Close manager: " + paramElement);
		EngineManager manager = (EngineManager) paramElement.getObjectValue();
    manager.close();
	}

	@Override
	public void notifyElementRemoved(Ehcache paramEhcache, Element paramElement) throws CacheException {
		log.info("Element removed from DB cache: " + paramElement);
		closeManager(paramElement);
	}

	@Override
	public void notifyElementExpired(Ehcache paramEhcache, Element paramElement) {
		log.info("Element expired from DB cache: " + paramElement);
		closeManager(paramElement);
	}

	@Override
	public void notifyElementEvicted(Ehcache paramEhcache, Element paramElement) {
		log.info("Element evicted from DB cache: " + paramElement);
		closeManager(paramElement);
	}

	@Override
	public void notifyElementPut(Ehcache paramEhcache, Element paramElement) throws CacheException {
		log.info("Element put: " + paramElement);
	}

	@Override
	public void notifyElementUpdated(Ehcache paramEhcache, Element paramElement) throws CacheException {
		log.info("Element updated: " + paramElement);
	}

	@Override
	public void notifyRemoveAll(Ehcache paramEhcache) {
		log.info("Remove all!");
	}

	@Override
	public void dispose() {
		log.info("Dispose!");
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
}
