package com.splout.db.hazelcast;

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

import java.io.Serializable;

import com.hazelcast.core.Hazelcast;

/**
 * Just keeps a tablespace name and a version. Created 
 * to be used as key on {@link Hazelcast} maps.
 */
@SuppressWarnings("serial")
public class TablespaceVersion implements Serializable {

	@Override
  public String toString() {
	  return "TablespaceVersion [tablespace=" + tablespace + ", version=" + version + "]";
  }

	private String tablespace;
	private long version;
	
	public TablespaceVersion() {
  }

	public TablespaceVersion(String tablespace, long version) {
	  super();
	  this.tablespace = tablespace;
	  this.version = version;
  }

	public String getTablespace() {
		return tablespace;
	}

	public void setTablespace(String tablespace) {
		this.tablespace = tablespace;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	@Override
  public int hashCode() {
	  final int prime = 31;
	  int result = 1;
	  result = prime * result + ((tablespace == null) ? 0 : tablespace.hashCode());
	  result = prime * result + (int) (version ^ (version >>> 32));
	  return result;
  }

	@Override
  public boolean equals(Object obj) {
	  if(this == obj)
		  return true;
	  if(obj == null)
		  return false;
	  if(getClass() != obj.getClass())
		  return false;
	  TablespaceVersion other = (TablespaceVersion) obj;
	  if(tablespace == null) {
		  if(other.tablespace != null)
			  return false;
	  } else if(!tablespace.equals(other.tablespace))
		  return false;
	  if(version != other.version)
		  return false;
	  return true;
  }

	
}
