package com.splout.db.integration;

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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.splout.db.common.SploutClient;
import com.splout.db.dnode.DNodeClient;
import com.splout.db.dnode.TestCommands;
import com.splout.db.thrift.DNodeException;
import com.splout.db.thrift.DNodeService;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;

/**
 * A simple program for killing some dnodes from a live cluster, for example to integrate test that everything keeps working.
 * It receives a parameter which is the number of DNodes to kill at once and another rate (in seconds) which is the time to wait
 * before killing and restarting the nodes again.
 */
public class DNodeKiller {

  @Parameter(required = true, names = {"-qnode"}, description = "The QNode from where we can get the list of DNodes. The list will only be asked once at startup.")
  private String qnode;

  @Parameter(required = true, names = {"-ndnodestokill"}, description = "Number of DNodes that will be randomly killed.")
  private Integer nDnodesToKill;

  @Parameter(required = true, names = {"-dnodekillrate"}, description = "I will kill [-ndnodestokill] DNodes for testing failover every as many seconds as specified. Then it will bring them back to life. And I tell you, this is going to last forever until you kill me.")
  private Long dNodeKillRate;

  public int run(String[] args) throws IOException {
    JCommander jComm = new JCommander(this);
    jComm.setProgramName("Splout DNode Killer");
    try {
      jComm.parse(args);
    } catch (ParameterException e) {
      System.out.println(e.getMessage());
      jComm.usage();
      return -1;
    } catch (Throwable t) {
      t.printStackTrace();
      jComm.usage();
      return -1;
    }

    SploutClient client = new SploutClient(qnode);
    List<String> dnodes = client.dNodeList();
    final int nDNodes = dnodes.size();

    // Kill DNodes every X seconds
    try {
      while (true) {
        Thread.sleep(dNodeKillRate * 1000);

        int badLuckForYou = (int) (Math.random() * nDNodes);

        for (int i = 0; i < nDnodesToKill; i++) {
          int dNodeIndex = badLuckForYou + i;
          if (dNodeIndex == dnodes.size()) {
            dNodeIndex = 0;
          }
          try {
            DNodeService.Client dNodeClient;
            dNodeClient = DNodeClient.get(dnodes.get(dNodeIndex));
            System.out.println("Killing: " + dnodes.get(dNodeIndex));
            dNodeClient.testCommand(TestCommands.SHUTDOWN.toString());
            // We just try, if the DNode is still there we kill it. Otherwise we do nothing.
            // This is better since if we had to ask each time to a QNode for the current DNodes we are forced to have
            // as many QNodes as DNodes (otherwise we are also killing QNodes!)
          } catch (DNodeException e) {
            e.printStackTrace();
          } catch (TException e) {
            e.printStackTrace();
          }
        }

        Thread.sleep(dNodeKillRate * 1000);

        // Now bring them back to life

        for (int i = 0; i < nDnodesToKill; i++) {
          int dNodeIndex = badLuckForYou + i;
          if (dNodeIndex == dnodes.size()) {
            dNodeIndex = 0;
          }
          try {
            DNodeService.Client dNodeClient;
            dNodeClient = DNodeClient.get(dnodes.get(dNodeIndex));
            System.out.println("Bringing back to life: " + dnodes.get(dNodeIndex));
            dNodeClient.testCommand(TestCommands.RESTART.toString());
          } catch (DNodeException e) {
            e.printStackTrace();
          } catch (TException e) {
            e.printStackTrace();
          }
        }
      }
    } catch (InterruptedException e) {
      System.out.println("OK, Bye bye!");
    }

    return 1;
  }

  public static void main(String[] args) throws IOException, DNodeException, TException {
    new DNodeKiller().run(args);
  }
}
