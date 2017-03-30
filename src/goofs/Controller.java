package goofs;

import java.io.*;
import java.lang.Thread;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.StringTokenizer;

/**
 * Controller for the goofs filesystem.
 * Responsible for tracking which chunk
 * servers contain which chunks and 
 * taking action to correct corruption
 * and downed chunk servers.
 *
 * @author Elliott Forney
 */
public class Controller
  extends Thread
{
  // number of replicas to maintain for each chunk
  public static final int replicationLevel = 3;

  // controller identification
  private HostID id;

  // server entries in a list
  private ArrayList<ControllerSEntry> sEntryList =
    new ArrayList<ControllerSEntry>();

  // server entries in a hash table mapped by host id
  private Hashtable<String, ControllerSEntry> sEntryByHost =
    new Hashtable<String, ControllerSEntry>();

  // lock for server data structures
  private Object sLock = new Object();

  // chunk entries in a hash table mapped by chunk descriptor
  private Hashtable<String, ControllerCEntry> cEntryByDesc =
    new Hashtable<String, ControllerCEntry>();

  // lock for chunk data structures
  private Object cLock = new Object();

  // reaper to remove entries for chunk servers
  // that are no longer responding
  ControllerReaper reaper = new ControllerReaper();

  /**
   * Create a new goofs controller.
   * @param port Port number to listen on
   */
  public Controller(int port)
    throws Exception
  {
    this.id = new HostID(port);

    // start cleanup thread
    reaper.start();
  }

  /**
   * Run controller.
   * Listen for connections and start message handlers
   * for each incomming message.
   */
  public void run()
  {
    try
    {
      // start new server socket on id's port
      ServerSocket ss = new ServerSocket(id.getPort());

      // loop, accepting new connections
      while (true)
        (new ControllerMessageHandler(ss.accept())).start();
    }
    catch (Exception e) {
      System.out.println("Error starting server socket: " +
                         e.getMessage() + " Aborting.");
      System.exit(1);
    }
  }

  // handle a message sent to controller
  private class ControllerMessageHandler
    extends MessageHandler
  {
    // create new controller message handler 
    public ControllerMessageHandler(Socket s)
    {
      super(s);
    }

    // handle a message sent to controller
    // and return a reply message
    public Message handle(Message m)
      throws Exception
    {
      // major heartbeat message
      if (m.kind == Message.Kind.MajorHeartbeat)
      {
        // cast to major heartbeat message
        MajorHeartbeatMessage mhbm =
          (MajorHeartbeatMessage)m;

        // get id for sender
        HostID cs = mhbm.getSource();

        // get all chunk descriptors currently on sender
        ChunkDescriptor[] chunkDesc = mhbm.getAllChunks();

        //
        ChunkDescriptor[] corruptDesc = mhbm.getCorruptChunks();

        //
        //cleanCorrupt(corruptDesc);

        // touch sending node and chunks
        touch(cs, chunkDesc);

        System.out.println("Major: " + m +
                           " Total: " + chunkDesc.length +
                           " Corrupt: " + corruptDesc.length);

        // update number of chunks in our map
        setNumChunks(cs, chunkDesc.length);

        // no reply expected
        return null;
      }

      // minor heartbeat message
      else if (m.kind == Message.Kind.MinorHeartbeat)
      {
        // cast to minor heartbeat message
        MinorHeartbeatMessage mhbm =
          (MinorHeartbeatMessage)m;

        // get id for sender
        HostID cs = mhbm.getSource();

        // get new chunk descriptors on sender
        ChunkDescriptor[] chunkDesc = mhbm.getNewChunks();

        //
        ChunkDescriptor[] corruptDesc = mhbm.getCorruptChunks();

        // touch sending node and chunks
        touch(cs, chunkDesc);

        System.out.println("Minor: " + m +
                           " New: " + chunkDesc.length +
                           " Corrupt: " + corruptDesc.length);

        // no reply expected
        return null;
      }

      // get chunkserver holding chunk associated with a 
      // given chunk descriptor
      else if (m.kind == Message.Kind.GetHolders)
      {
        // cast message
        GetHoldersMessage ghm = (GetHoldersMessage)m;

        // get chunkservers holding chunk
        ghm.setChunkServers(queryChunk(ghm.getChunkDescriptor()));

        // swap source and dest and send back
        ghm.swapSrcDst();
        return ghm;
      }

      // get chunkservers that are eligible to hold new chunks
      else if (m.kind == Message.Kind.GetFree)
      {
        // cast message
        GetFreeMessage gfm = (GetFreeMessage)m;

        // get eligible chunkservers
        gfm.setChunkServers(getFree());

        // swap source and dest and send back
        gfm.swapSrcDst();
        return gfm;
      }

      // inappropriate message kind
      else
        throw new Exception("Inappropriate message kind.");
    }
  }

  // thread to remove server entries that
  // have not responded for a period of time
  private class ControllerReaper
    extends Thread
  {
    // seconds to sleep between passes of reaper
    private static final int deathSleepSecs = 20;

    // start reaping
    public void run()
    {
      while (true)
      {
        // check for expired server entries
        reap();

        // check for under-replicated chunks
        //checkReplication();

        try {
          // block until past next heartbeat
          Thread.sleep(deathSleepSecs*1000);
        }
        catch (Exception e) {
          System.out.println("Reaper sleep interrupted.");
        }
      }
    }

    // remove entries that have not
    // responed for a period of time
    public synchronized void reap()
    {
      // list of server entries to delete
      ArrayList<ControllerSEntry> delList =
        new ArrayList<ControllerSEntry>();

      synchronized (sLock)
      {
        // create iterator over entry list
        ListIterator<ControllerSEntry> itr =
          sEntryList.listIterator();

        // while we have more entries
        while (itr.hasNext())
        {
          // get current entry
          ControllerSEntry curEntry = itr.next();

          // if entry has expired
          if (curEntry.hasExpired())
          {
            // send message to console
            System.out.println("ChunkServer " + curEntry.getID() +
                              " appears dead.  Removing from list.");

            delList.add(curEntry);
          }
        }

        // delete each expired server entry
        itr = delList.listIterator();
        while (itr.hasNext())
        {
          ControllerSEntry curSEntry = itr.next();

          // delete server entry
          delSEntry(curSEntry);

          // remove chunks on this chunkserver from CEntries
          synchronized (cLock) {
            cleanCEntries(curSEntry);
          }
        }
      }
    }

    // check for under-replicated chunks and fix
    // kinda hairy here but it works
/*    private void checkReplication()
    {
      synchronized (cLock)
      {
        Iterator<ControllerCEntry> itr =
          cEntryByDesc.values().iterator();

        // iterate over chunk entries
        while (itr.hasNext())
        {
          ControllerCEntry curCEntry =
            itr.next();

          System.out.println("For centry " + curCEntry.getDescriptor() + " numreplicas " + curCEntry.getNumChunkServers());

          // if chunk doesn't have enough replicas
          if (curCEntry.getNumChunkServers() < replicationLevel)
          {
            HostID[] curHolders = curCEntry.getChunkServers();

            synchronized (sLock)
            {
              HostID newHolder = null;

              // look at each server entry
              for (int i = 0; (i < sEntryList.size()) && (newHolder == null); ++i)
              {
                newHolder = sEntryList.get(i).getID();

                // check if server entry is a holder of chunk
                for (int j = 0; (j < curHolders.length) && (newHolder != null); ++j)
                  if (newHolder.equals(curHolders[j]))
                    newHolder = null;
              }
              if (newHolder == null)
                System.out.println("No available chunkservers to fix replication of " + curCEntry.getDescriptor());
              else
                {}//fixReplication(curHolders[0], newHolder, curCEntry.getDescriptor());
            }
          }
        }
      }
    } */
  }

  //
  private HostID[] getChunkServers()
  {
    HostID[] chunkServers;

    synchronized (sLock)
    {
      chunkServers = new HostID[sEntryList.size()];

      for (int i = 0; i < chunkServers.length; ++i)
        chunkServers[i] = sEntryList.get(i).getID();
    }

    return chunkServers;
  }

  // Get chunkservers that are eligible to store new chunks.
  private HostID[] getFree()
  {
    // array to hold return value
    HostID[] free;

    // if we have fewer than replicationLevel chunkservers
    // available, we will return fewer
    if (sEntryList.size() < Controller.replicationLevel)
      free = new HostID[sEntryList.size()];
    else
      free = new HostID[Controller.replicationLevel];

    // lock our server data structures
    synchronized (sLock)
    {
      // sort by number of stored chunks
      // this would definitely be a bottleneck
      // for scaling, need a better hueristic.
      Collections.sort(sEntryList);

      // get top entries from list
      for (int i = 0; i < free.length; ++i)
      {
        free[i] = sEntryList.get(i).getID();

        // assume we will be adding new chunks,
        // fixed on major heartbeat if not
        sEntryList.get(i).incrementNumChunks();
      }
    }

    return free;
  }

  // Find id's of chunk servers currently holding a copy
  // of a chunk described by a given chunk descriptor.
  private HostID[] queryChunk(ChunkDescriptor cd)
  {
    HostID[] holders;

    synchronized (cLock)
    {
      ControllerCEntry cEntry =
        cEntryByDesc.get(cd.toSeqString());

      if (cEntry == null)
        holders = new HostID[0];
      else
        holders = cEntry.getChunkServers();
    }

    return holders;
  }

  // Update timestamp or create server entry
  // for given chunkserver id and add chunk
  // entries to table if they don't already
  // exist.
  private void touch(HostID cs, ChunkDescriptor[] chunkDesc)
  {
    synchronized (sLock)
    {
      // lookup entry by chunkserver id
      ControllerSEntry curSEntry =
        sEntryByHost.get(cs.toString());

      // if entry was not in table
      if (curSEntry == null)
      {
        // add a new server entry for
        // given chunkserver id
        addSEntry(cs, chunkDesc);

        // request a major heartbeat
        try {
          requestMajor(cs);
        }
        catch (Exception e) {
          System.out.println(e.getMessage());
        }
      }

      else
        // otherwise, update timestamp
        curSEntry.touch(chunkDesc);
    }

    for (int i = 0; i < chunkDesc.length; ++i)
    {
      synchronized (cLock)
      {
        ControllerCEntry curCEntry =
          cEntryByDesc.get(chunkDesc[i].toSeqString());

        if (curCEntry == null)
        {
          ControllerCEntry newCEntry =
            new ControllerCEntry(chunkDesc[i], cs);

          cEntryByDesc.put(chunkDesc[i].toSeqString(), newCEntry);
        }
        else
          curCEntry.touch(cs);
      }
    }
  }

  // Set the number of chunks currently stored on
  // a given chunkserver.
  private void setNumChunks(HostID cs, int numChunks)
  {
    synchronized (sLock)
    {
      // lookup server entry by chunkserver id
      ControllerSEntry curSEntry =
        sEntryByHost.get(cs.toString());

      // set number of chunks
      curSEntry.setNumChunks(numChunks);
    }
  }

  //
  private void requestMajor(HostID cs)
    throws Exception
  {
    RequestMajorMessage rmm =
      new RequestMajorMessage(id, cs);

    try {
      rmm.send();
    }
    catch (Exception e) {
      throw new Exception("Failed to send major heartbeat request: " +
                          e.getMessage());
    }
  }

  //
  private void requestAllMajor()
    throws Exception
  {
    HostID[] chunkServer = getChunkServers();

    for (int i = 0; i < chunkServer.length; ++i)
      requestMajor(chunkServer[i]);
  }

  //
  private void requestMinor(HostID cs)
    throws Exception
  {
    RequestMinorMessage rmm =
      new RequestMinorMessage(id, cs);

    try {
      rmm.send();
    }
    catch (Exception e) {
      throw new Exception("Failed to send minor heartbeat request: " +
                          e.getMessage());
    }
  }

  //
  private void requestAllMinor()
    throws Exception
  {
    HostID[] chunkServer = getChunkServers();

    for (int i = 0; i < chunkServer.length; ++i)
      requestMinor(chunkServer[i]);
  }

  //
  private void fixReplication(HostID curHolder, HostID newHolder, ChunkDescriptor chunkDesc)
  {
    FixReplicationMessage frm =
      new FixReplicationMessage(id, curHolder, newHolder, chunkDesc);

    System.out.println("Fixing replication: Sending " + chunkDesc +
                       " from " + curHolder + " to " + newHolder);

    synchronized (sLock) {
      // very ineffecient but keeps balance
      Collections.sort(sEntryList);
      sEntryByHost.get(newHolder.toString()).incrementNumChunks();
    }

    try {
      frm.send();
    }
    catch (Exception e) {
      System.out.println("Failed to send fix replication message: " +
                         e.getMessage());
    }
  }

  // add server entry for given chunkserver id
  // Must be synchronized by caller!
  private void addSEntry(HostID cs, ChunkDescriptor[] chunkDesc)
  {
    ControllerSEntry newEntry =
      new ControllerSEntry(cs, chunkDesc);

    sEntryByHost.put(cs.toString(), newEntry);
    sEntryList.add(newEntry);
  }

  // delete given server entry from all data structure
  // Must be synchronized by caller!
  private void delSEntry(ControllerSEntry e)
  {
    // remove server entry from hash table by id
    sEntryByHost.remove(e.getID().toString());

    // create iterator over entry list
    ListIterator<ControllerSEntry> itr =
      sEntryList.listIterator();

    // while we have more entries
    while (itr.hasNext())
    {
      // get current entry
      ControllerSEntry curSEntry = itr.next();

      // if this is the entry we are looking for
      if (curSEntry.equals(e))
      {
        // remove from list
        itr.remove();
        break; // we're done
      }
    }
  }

  //
  // Must be synchronized by caller!
  private void cleanCEntries(ControllerSEntry sentry)
  {
    HostID cs = sentry.getID();
    ChunkDescriptor[] chunkDesc = sentry.getChunks();

    // for each chunk descriptor
    for (int i = 0; i < chunkDesc.length; ++i)
    {
      // get centry by chunk descriptor
      ControllerCEntry curCEntry =
        cEntryByDesc.get(chunkDesc[i].toSeqString());

      // remove chunkserver from centry
      curCEntry.delChunkServer(cs);

      //
      HostID[] curHolders = curCEntry.getChunkServers();

      //
      HostID newHolder = null;

      // look at each server entry
      for (int j = 0; (j < sEntryList.size()) && (newHolder == null); ++j)
      {
        newHolder = sEntryList.get(j).getID();

        // check if server entry is a holder of chunk
        for (int k = 0; (k < curHolders.length) && (newHolder != null); ++k)
          if (newHolder.equals(curHolders[k]))
            newHolder = null;
      }
      if (newHolder == null)
        System.out.println(
          "No available chunkservers to fix replication of " +
          curCEntry.getDescriptor() + ".");
      else
        fixReplication(curHolders[0], newHolder, curCEntry.getDescriptor());
    }
  }

  //
/*  private void cleanCorrupt(ChunkDescriptor[] chunkDesc)
  {
    synchronized (cLock)
    {
      for (i = 0; i < chunkDesc.length; ++i)
      {
        ControllerCEntry centry =
          cEntryByDesc.get(chunkDesc[i].toSeqString());

        centry.delChunkServer(chunkDesc[i]);
      }
    }
  } */

  // Generate human readable string
  // listing all current chunkservers.
  private String getSEntryString()
  {
    String s = "";

    synchronized (sLock)
    {
      ListIterator<ControllerSEntry> itr =
        sEntryList.listIterator();
      while (itr.hasNext())
        s += itr.nextIndex() + ") " + itr.next() + "\n";
    }

    return s;
  }

  //
  private String getCEntryString()
  {
    String s = "";

    synchronized (cLock)
    {
      Iterator<ControllerCEntry> itr =
        cEntryByDesc.values().iterator();

      int i = 0;
      while (itr.hasNext())
        s += i++ + ") " + itr.next() + "\n";
    }

    return s;
  }

  /**
   * Start a new controller
   * @param args Command-line arguments
   */
  public static void main(String[] args)
  {
    int contPort = 8000; // controller port number

    try
    {
      // for each command-line argument
      for (int i = 0; i < args.length; ++i)
      {
        // set controller port number
        if (args[i].equals("--port"))
          contPort = Integer.parseInt(args[++i]);

        // bad command line argument
        else
          throw new Exception("Bad argument.");
      }
    }
    catch (Exception e)
    {
      System.out.println("Error processing command line arguments: " +
                         e.getMessage());
      System.exit(1);
    }

    try
    {
      // initialize new controller
      Controller cont = new Controller(contPort);

      // start controller thread
      cont.start();

      // start command prompt
      prompt(cont);

      // wait for controller thread to exit
      cont.join();
    }
    catch (Exception e) {
      System.out.println("Controller error: " +
                         e.getMessage() + ": Aborting.");
      System.exit(1);
    }
  }

  // give command prompt and process commands
  private static void prompt(Controller cont)
  {
    // start buffered reader on console
    BufferedReader input = new BufferedReader(
      new InputStreamReader(System.in));

    // loop, waiting for commands
    while (true)
    {
      // print prompt
      System.out.print("controller> ");
      try
      {
        // get input line and desensitize case
        String line = input.readLine();

        // start space deliminated tokenizer
        StringTokenizer tok = new StringTokenizer(line);

        // if blank line, we're done
        String cmd = null;
        if (!tok.hasMoreElements())
          continue;

        // command is first token
        else
          cmd = tok.nextToken();

        // print some simple help
        if (cmd.equals("?"))
          System.out.println(
            "clear centry free holders id major minor reap sentry quit");

        // clear the console
        else if (cmd.equals("clear"))
          System.out.print("\033[2J\033[H");

        //
        else if (cmd.equals("centry"))
          System.out.println(cont.getCEntryString());

        // list chunkservers eligible to hold new chunks
        else if (cmd.equals("free"))
        {
          HostID[] free = cont.getFree();
          for (int i = 0; i < free.length; ++i)
            System.out.println(i + ") " + free[i]);
          System.out.println();
        }

        // list chunkservers holding chunk descriptor
        else if (cmd.equals("holders"))
        {
          if (!tok.hasMoreElements())
            System.out.println("holders filename offset");

          else
          {
            File f = new File(tok.nextToken());

            if (!tok.hasMoreElements())
              System.out.println("holders filename offset");

            else
            {
              int offset = Integer.parseInt(tok.nextToken());

              HostID[] holders = cont.queryChunk(
                new ChunkDescriptor(f, offset));

              for (int i = 0; i < holders.length; ++i)
                System.out.println(i + ") " + holders[i]);
              System.out.println();
            }
          }
        }

        // print controller's identification
        else if (cmd.equals("id"))
          System.out.println(cont.id);

        // request each chunkserver to send
        // a major heartbeat now
        else if (cmd.equals("major"))
          cont.requestAllMajor();

        // request each chunkserver to send
        // a minor heartbeat now
        else if (cmd.equals("minor"))
          cont.requestAllMinor();

        else if (cmd.equals("reap"))
          cont.reaper.reap();

        //
        else if (cmd.equals("sentry"))
          System.out.println(cont.getSEntryString());

        // quit controller
        else if (cmd.equals("quit"))
          // easy but brutal & bad practice
          System.exit(0);

        // bad command
        else
          System.out.println("Bad command: " + cmd);
      }
      catch (Exception e) {
        System.out.println("Command failed: " + e.getMessage());
      }
    }
  }
}
