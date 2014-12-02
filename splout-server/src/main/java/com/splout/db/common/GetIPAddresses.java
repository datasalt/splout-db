package com.splout.db.common;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedList;

/**
 * From: http://pastebin.com/5X073pUc
 */
public class GetIPAddresses {

  private final static Log log = LogFactory.getLog(GetIPAddresses.class);

  /**
   * Returns all available IP addresses.
   * <p/>
   * In error case or if no network connection is established, we return an empty list here.
   * <p/>
   * Loopback addresses are excluded - so 127.0.0.1 will not be never returned.
   * <p/>
   * The "primary" IP might not be the first one in the returned list.
   *
   * @return Returns all IP addresses (can be an empty list in error case or if network connection is missing).
   * @throws SocketException
   * @since 0.1.0
   */
  public static Collection<InetAddress> getAllLocalIPs() throws SocketException {
    LinkedList<InetAddress> listAdr = new LinkedList<InetAddress>();

    Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
    if (nifs == null)
      return listAdr;

    while (nifs.hasMoreElements()) {
      NetworkInterface nif = nifs.nextElement();
      // We ignore subinterfaces - as not yet needed.
      Enumeration<InetAddress> adrs = nif.getInetAddresses();
      while (adrs.hasMoreElements()) {
        InetAddress adr = adrs.nextElement();
        if (adr != null && !adr.isLoopbackAddress()
            && (nif.isPointToPoint() || !adr.isLinkLocalAddress())) {
          log.info("Available site local address: " + adr);
          listAdr.add(adr);
        }
      }
    }
    return listAdr;
  }

  public static void main(String[] args) throws SocketException {
    GetIPAddresses.getAllLocalIPs();
  }
}
