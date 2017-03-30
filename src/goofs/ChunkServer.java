package goofs;

import java.io.*;
import java.lang.Thread;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.ListIterator;
import java.util.StringTokenizer;

/**
 * Storage node in a goofs filesystem.
 *
 * @author Elliott Forney
 */
public class ChunkServer
  extends Thread
{
  // seconds between minor heartbeats
  public static final int minorHeartbeatSecs = 10;

  // minor heartbeats between major heartbeats
  public static final int majorHeartbeatInterval = 5;

  // chunkserver indentification
  HostID csID;

  // controller identification
  HostID contID;

  // list of descriptors for all chunks on this node
  private ArrayList<ChunkDescriptor> chunksList =
    new ArrayList<ChunkDescriptor>();  

  // table of descriptors for all chunks on this node
  private Hashtable<String, ChunkDescriptor> chunksTable =
    new Hashtable<String, ChunkDescriptor>();

  // lock for chunks data structures
  private Object chunksLock = new Object();

  // list of descriptors for all chunks added to this
  // node since the last minor heartbeat was sent
  private ArrayList<ChunkDescriptor> newChunks =
    new ArrayList<ChunkDescriptor>();

  //
  private ArrayList<ChunkDescriptor> corruptChunks =
    new ArrayList<ChunkDescriptor>();

  /**
   *  Create new goofs storage node.
   *
   * @param csID Identification for this chunkserver
   * @param contID Identification for controller to use
   */
  public ChunkServer(HostID csID, HostID contID)
  {
    this.csID   = csID;
    this.contID = contID;
  }

  /**
   * Run chunkserver.
   * Listen for connections and start message handlers
   * for each incomming message.
   */
  public void run()
  {
    //
    ChunkServerHeart heart = new ChunkServerHeart();
    heart.start();

    try
    {
      // start new server socket on id's port
      ServerSocket ss = new ServerSocket(csID.getPort());

      // loop, accepting new connections
      while (true)
        (new ChunkServerMessageHandler(ss.accept())).start();
    }
    catch (Exception e) {
      System.out.println("Error starting server socket: " +
                         e.getMessage() + " Aborting.");
      System.exit(1);
    }
  }

  /**
   *
   */
  public void sendMajorHeartbeat()
    throws Exception
  {
    MajorHeartbeatMessage majorHeartbeat;

    synchronized (chunksLock)
    {
      synchronized (corruptChunks)
      {
        // create a new major heartbeat message
        majorHeartbeat =
          new MajorHeartbeatMessage(csID, contID,
            chunksList.toArray(new ChunkDescriptor[0]),
            corruptChunks.toArray(new ChunkDescriptor[0]));
      }
    }

    try {
      // send major heartbeat message
      majorHeartbeat.send();

      // clear new chunks list
      synchronized (newChunks) {
        newChunks.clear();
      }

      synchronized (corruptChunks) {
        corruptChunks.clear();
      }
    }
    catch (Exception e) {
      throw new Exception("Failed to send major heartbeat: " +
                          e.getMessage());
    }
  }

  /**
   *
   */
  public void sendMinorHeartbeat()
    throws Exception
  {
    MinorHeartbeatMessage minorHeartbeat;

    synchronized (newChunks)
    {
      synchronized (corruptChunks)
      {
        // create a new minor heartbeat message
        minorHeartbeat =
          new MinorHeartbeatMessage(csID, contID,
            newChunks.toArray(new ChunkDescriptor[0]),
            corruptChunks.toArray(new ChunkDescriptor[0]));
      }
    }

    try {
      // send minor heartbeat message
      minorHeartbeat.send();

      // clear new chunks list
      synchronized (newChunks) {
        newChunks.clear();
      }

      synchronized (corruptChunks) {
        corruptChunks.clear();
      }
    }
    catch (Exception e) {
      throw new Exception("Failed to send minor heartbeat: " +
                          e.getMessage());
    }
  }

  // check if chunk is currently stored here
  // must be synchronized by parent
  private boolean exists(ChunkDescriptor chunkDesc)
  {
    return chunksTable.containsKey(chunkDesc.toSeqString());
  }

  // submit a new chunk
  private void submit(ChunkDescriptor chunkDesc, byte[] data)
  {
    synchronized (chunksLock)
    {
      try
      {
        // working chunk
        Chunk c;

        // see if a chunk already exists
        // for the given descriptor
        if (exists(chunkDesc))
        {
          // read the existing chunk
          c = Chunk.read(chunkDesc);

          // get data from existing chunk
          byte[] readData = c.getData();

          // if existing chunk had more data
          if (readData.length > data.length)
          {
            //System.arraycopy(sourceArray, sourceStartIndex, targetArray, targetStartIndex, length);

            // copy current data on top of start of previous data
            System.arraycopy(data, 0, readData, 0, data.length);

            //
            c.setData(readData);
          }
          else
            //
            c.setData(data);
        }
        else
          // create a new chunk
          c = new Chunk(chunkDesc, data);

        // write the chunk to disk
        c.write();
      }
      catch (Exception e)
      {
        System.out.println("Failed to submit chunk " + chunkDesc +
                           ": " + e.getMessage());
        return;
      }

      // if chunk is not already listed, i.e. new chunk
      if (!chunksTable.containsKey(chunkDesc.toSeqString()))
      {
        // add chunk descriptor to list
        chunksList.add(chunkDesc);

        // add chunk descriptor to table
        chunksTable.put(chunkDesc.toSeqString(), chunkDesc);

        synchronized (newChunks) {
          // add to new chunks list
          newChunks.add(chunkDesc);
        }
      }
    }
  }

  // retrieve an existing chunk
  private byte[] retrieve(ChunkDescriptor chunkDesc)
    throws Exception
  {
    byte[] data = null;

    synchronized (chunksLock)
    {
      boolean success = false;

      while (!success)
      {
        try
        {
          Chunk c = Chunk.read(chunkDesc);
          data = c.getData();
          success = true;
        }
        catch (Exception e)
        {
          //
          System.out.println(e.getMessage());

          try {
            handleCorruption(chunkDesc);
          }
          catch (Exception ec) {
            throw new Exception("Failed to correct corruption: " +
                                ec.toString());
          }
        }
      }
    }

    return data;
  }

  // synchronized by caller
  private void handleCorruption(ChunkDescriptor chunkDesc)
    throws Exception
  {
    // add to corrupted list
    synchronized (corruptChunks) {
      corruptChunks.add(chunkDesc);
    }

    GetHoldersMessage ghm =
      new GetHoldersMessage(csID, contID, chunkDesc);

    try {
      ghm = ghm.send();
    }
    catch (Exception e) {
      throw new Exception("Failed to contact controller: " +
                          e.getMessage());
    }

    HostID[] holders = ghm.getChunkServers();

    ArrayList<HostID> otherHolders =
      new ArrayList<HostID>();

    for (int i = 0; i < holders.length; ++i)
      if (!holders[i].equals(csID))
        otherHolders.add(holders[i]);
        
    if (otherHolders.size() == 0)
      throw new Exception("No other chunkservers hold " +
                          chunkDesc + ".");

    FixCorruptionMessage fcm =
      new FixCorruptionMessage(
        csID, otherHolders.toArray(new HostID[0]), chunkDesc);

    try {
      fcm = fcm.send();
    }
    catch (Exception e) {
      throw new Exception("Failed to retrieve " + chunkDesc +
                          ": " + e.getMessage());
    }

    fcm.getChunk().write();
  }


  // remove all chunks from disk and quit
  private void quit()
  {
    synchronized (chunksLock)
    {
      //
      ListIterator<ChunkDescriptor> itr =
        chunksList.listIterator();

      //
      while (itr.hasNext())
      {
        //
        ChunkDescriptor curDesc =
          itr.next();

        //
        Chunk.delete(curDesc);

        //
        itr.remove();

        //
        chunksTable.remove(curDesc.toSeqString());
      }

      //
      System.exit(0);
    }
  }

  // thread to periodically send heartbeats to controller
  private class ChunkServerHeart
    extends Thread
  {
    // heartbeats thus far mod 32000
    private int heartbeatCount = 0;

    // run heartbeat thread
    public void run()
    {
      // loop indefinitely
      while (true)
      {
        // increment heartbeat counter
        // used to tell when to send major heartbeats
        // modulo just prevents overflow
        heartbeatCount = (heartbeatCount+1) % 32000;

        try
        {
          // if time for a major heartbeat
          if ((heartbeatCount % majorHeartbeatInterval) == 0)
            sendMajorHeartbeat();

          // time for minor heartbeat
          else
            sendMinorHeartbeat();
        }
        catch (Exception e) {
          System.out.println("Heart skipped a beat: " + e.getMessage());
        }

        try {
          // block until time for next heartbeat
          Thread.sleep(minorHeartbeatSecs*1000);
        }
        catch (Exception e) {
          // do nothing
        }
      }
    }
  }

  // handle a message send to chunkserver
  private class ChunkServerMessageHandler
    extends MessageHandler
  {
    // create a new chunkserver message handler
    public ChunkServerMessageHandler(Socket s)
    {
      super(s);
    }

    // handle message sent to chunkserver
    // and construct reply
    public Message handle(Message m)
      throws Exception
    {
      System.out.println("Recieved message: " + m);

      // message requesting chunkserver store new chunk
      if (m.kind == Message.Kind.SubmitData)
      {
        // cast message
        SubmitDataMessage sdm = (SubmitDataMessage)m;

        // write chunk to disk
        submit(sdm.getChunkDescriptor(), sdm.getData());

        // set next message destination
        // if it has one
        if (sdm.setNextDest())
        {
          // set source to this chunkserver
          m.setSource(csID);

          // send to next destination
          try {
            sdm.send();
          }
          catch (Exception e) {
            System.out.println("Unable to forward data: " +
                               sdm.getChunkDescriptor() + ": " + e.getMessage());
          }
        }

        // no reply expected
        return null;
      }

      //
      else if (m.kind == Message.Kind.RetrieveData)
      {
        // cast message
        RetrieveDataMessage rdm = (RetrieveDataMessage)m;

        // message reply
        RetrieveDataMessage reply;

        // get data from chunk
        // will be null if error or corruption
        byte[] data = retrieve(rdm.getChunkDescriptor());

        // if corruption or error
        if (data == null)
        {
          // if we have forwarding options
          if (rdm.setNextDest())
          {
            try {
              // try to forward request
              reply = rdm.send();
            }
            catch (Exception e) {
              System.out.println("Unable to forward request: " +
                                 rdm.getChunkDescriptor() + ": " + e.getMessage());
              // return failure message if we can't
              reply = rdm;
            }

          }

          // no other options, return message
          // data is null to indicate failure
          else
            reply = rdm;
        }
        else
        {
          // we got the data, set it
          rdm.setData(data);
          reply = rdm;
        }

        // swap source and dest and send back
        reply.swapSrcDst();
        return reply;
      }

      //
      else if (m.kind == Message.Kind.FixCorruption)
      {
        // cast message
        FixCorruptionMessage fcm = (FixCorruptionMessage)m;

        fcm.setChunk(Chunk.read(fcm.getChunkDescriptor()));

        // swap source and dest and send back
        fcm.swapSrcDst();
        return fcm;
      }

      // send minor heartbeat on request
      else if (m.kind == Message.Kind.RequestMinor)
      {
        sendMinorHeartbeat();
        return null;
      }

      // send major heartbeat on request
      else if (m.kind == Message.Kind.RequestMajor)
      {
        sendMajorHeartbeat();
        return null;
      }

      //
      else if (m.kind == Message.Kind.FixReplication)
      {
        FixReplicationMessage frm = (FixReplicationMessage)m; 

        ChunkDescriptor chunkDesc = frm.getChunkDescriptor();

        byte[] data = retrieve(chunkDesc);

        HostID[] newHolder = new HostID[1];
        newHolder[0] = frm.getNewHolder();

        SubmitDataMessage sdm =
          new SubmitDataMessage(csID, newHolder,
                                chunkDesc, data);

        try {
          // send chunk submission
          sdm.send();
        }
        catch (Exception e) {
          System.out.println("Unable to forward data: " +
                              chunkDesc + ": " + e.getMessage());
        }

        // no reply expected
        return null;
      }

      // inappropriate message kind
      else
        throw new Exception("Inappropriate message kind.");
    }
  }

  /**
   * Start a new chunkserver
   * @param args Command-line arguments
   */
  public static void main(String[] args)
  {
    int    csPort   = 8000;       // chunkserver port number
    int    contPort = 8000;       // controller port number
    String contHost = "horde-0";  // controller host name

    try
    {
      // for each command-line argument
      for (int i = 0; i < args.length; ++i)
      {
        // set chunkserver port number
        if (args[i].equals("--port"))
          csPort = Integer.parseInt(args[++i]);

        // set controller port number
        else if (args[i].equals("--cont-port"))
          contPort = Integer.parseInt(args[++i]);

        // set controller host name
        else if (args[i].equals("--cont-host"))
          contHost = args[++i];

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
      // initialize new chunkserver
      ChunkServer cs = new ChunkServer(new HostID(csPort),
                                       new HostID(contHost, contPort));

      // start chunkserver thread
      cs.start();

      // start command prompt
      prompt(cs);

      // wait for controller thread to exit
      cs.join();
    }
    catch (Exception e) {
      System.err.println("Chunk server error: " +
                         e.getMessage() + ": Aborting.");
      System.exit(1);
    }
  }

  // give command prompt and process commands
  private static void prompt(ChunkServer cs)
  {
    // start buffered reader on console
    BufferedReader input = new BufferedReader(
      new InputStreamReader(System.in));

    // loop, waiting for commands
    while (true)
    {
      // print prompt
      System.out.print("chunkserver> ");
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
          System.out.println("id chunks clear cont major minor quit");

        // print chunkserver's identification
        else if (cmd.equals("id"))
          System.out.println(cs.csID);

        // list all chunks currently stored
        else if (cmd.equals("chunks"))
        {
          synchronized (cs.chunksList)
          {
            for (int i = 0; i < cs.chunksList.size(); ++i)
              System.out.println(i + ") " +
                                 cs.chunksList.get(i));
          }
        }

        // clear the console
        else if (cmd.equals("clear"))
          System.out.print("\033[2J\033[H");

        // print controller's identification
        else if (cmd.equals("cont"))
          System.out.println(cs.contID);

        // send major heartbeat
        else if (cmd.equals("major"))
          cs.sendMajorHeartbeat();

        // send minor heartbeat
        else if (cmd.equals("minor"))
          cs.sendMinorHeartbeat();

        // quit chunkserver
        else if (cmd.equals("quit"))
          cs.quit();

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
