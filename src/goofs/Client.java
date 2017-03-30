package goofs;

import java.io.*;
import java.util.ArrayList;

/**
 * Submits, retrieves and modifies files
 * stored in a goofs filesystem.
 *
 * @author Elliott Forney
 */
public class Client
{
  private static enum ModeKind {
    None, Submit, Receive
  }

  // id for client
  private HostID clientID;

  // id of controller this client will contact
  private HostID controllerID;

  /**
   *  Create a new goofs client
   * @param controllerID ID of controller to contact
   */
  public Client(HostID controllerID)
    throws Exception
  {
    this.controllerID = controllerID;
    this.clientID = new HostID("client", 8000);
  }

  // submit a new file
  private void submitFile(File f)
    throws Exception
  {
    // open a new input stream on file
    FileInputStream fis = new FileInputStream(f);

    // byte data for file
    byte[] fileData = new byte[(int)f.length()];

    try {
      // read bytes from file
      fis.read(fileData);

      // close input stream
      fis.close();
    }
    catch (Exception e) {
      throw new Exception("Error reading file: " + e.getMessage());
    }

    // number of chunks
    int numChunks = fileData.length / Chunk.maxSize;

    // size of trailing chunk
    int lastChunkSize = fileData.length % Chunk.maxSize;

    // if we have a trailing chunk
    if (lastChunkSize > 0)
      // we have one extra chunk
      ++numChunks;

    else
      // otherwise, last chunk is full sized
      lastChunkSize = Chunk.maxSize;

    //
    if (numChunks == 0)
      throw new Exception("Cannot submit empty file.");

    System.out.println("Breaking " + f + " into " + numChunks + " blocks.");
    System.out.println("Trailing block size is " + lastChunkSize + " bytes");

    // array holding all chunk descriptors
    ChunkDescriptor[] chunkDesc = new ChunkDescriptor[numChunks];

    // two-dim array holding chunk, data
    byte[][] chunkData = new byte[numChunks][];
    for (int i = 0; i < numChunks-1; ++i)
      chunkData[i] = new byte[Chunk.maxSize];
    chunkData[numChunks-1] = new byte[lastChunkSize];

    // for each chunk except trailing
    int curChunk;
    for (curChunk = 0; curChunk < numChunks-1; ++curChunk)
    {
      int offset = curChunk*Chunk.maxSize;
      chunkDesc[curChunk] = new ChunkDescriptor(f, offset);

      /*
      // for each byte to store in current chunkData element
      for (int curByte = 0; curByte < Chunk.maxSize; ++curByte)
        // grab chunk byte from file bytes
        chunkData[curChunk][curByte] = fileData[offset+curByte];    
      */
      System.arraycopy(fileData, offset,
                       chunkData[curChunk], 0, Chunk.maxSize);
    }

    // take care of trailing chunk
    {
      int offset = curChunk*Chunk.maxSize;
      chunkDesc[curChunk] = new ChunkDescriptor(f, offset);

      /*
      for (int curByte = 0; curByte < lastChunkSize; ++curByte)
        chunkData[curChunk][curByte] =
          fileData[(curChunk*Chunk.maxSize)+curByte];
      */
      System.arraycopy(fileData, curChunk*Chunk.maxSize,
                       chunkData[curChunk], 0, lastChunkSize);
    }

    // throw an exception if we didn't wind up at the end
    if (++curChunk != numChunks)
      throw new Exception("Failed to initialize all chunks.");

    // submit each chunk to goofs filesystem
    for (int i = 0; i < numChunks; ++i)
    {
      // message to ask controller for proper chunkservers
      GetHoldersMessage ghm =
        new GetHoldersMessage(clientID, controllerID, chunkDesc[i]);

      try {
        // send request for chunkservers
        ghm = ghm.send();
      }
      catch (Exception e) {
        throw new Exception("Failed to contact controller: " + e.getMessage());
      }

      // pull the chunkserver id's from the reply
      HostID[] chunkServers = ghm.getChunkServers();

      if (chunkServers.length == 0)
      {
        System.out.println("New chunk going to: ");

        GetFreeMessage gfm =
          new GetFreeMessage(clientID, controllerID);

        try {
          gfm = gfm.send(); 
        }
        catch (Exception e) {
          throw new Exception("Failed to contact controller: " + e.getMessage());
        }

        chunkServers = gfm.getChunkServers();

        if (chunkServers.length == 0)
          throw new Exception("No chunkservers available.");
      }
      else
        System.out.println("Existing chunk going to: ");

      for (int j = 0; j < chunkServers.length; ++j)
        System.out.println(j + ") " + chunkServers[j]);

      // message to submit chunk to chunkserver
      SubmitDataMessage sdm =
        new SubmitDataMessage(clientID, chunkServers, chunkDesc[i], chunkData[i]);

      try {
        // send chunk submission
        sdm.send();
      }
      catch (Exception e) {
        throw new Exception("Failed to contact any chunkserver at " + chunkDesc[i] +
                            ": " + e.getMessage());
      }
    }
  }

  // retrieve a file already stored
  private void retrieveFile(File f)
    throws Exception
  {
    int offset = 0;
    ChunkDescriptor curDesc = new ChunkDescriptor(f, offset);
    ArrayList<Byte> fileData = new ArrayList<Byte>();

    // message to ask controller for proper chunkservers
    GetHoldersMessage ghm =
      new GetHoldersMessage(clientID, controllerID, curDesc);

    try {
      // send request for chunkservers
      ghm = ghm.send();
    }
    catch (Exception e) {
      throw new Exception("Failed to contact controller: " + e.getMessage());
    }

    // pull the chunkserver id's from the reply
    HostID[] holders = ghm.getChunkServers();

    if (holders.length == 0)
    {
      System.out.println("File " + f + " not found in filesystem.");
      return;
    }

    do
    {
      RetrieveDataMessage rdm =
        new RetrieveDataMessage(clientID, holders, curDesc);

      try {
        rdm = rdm.send();
      }
      catch (Exception e) {
        throw new Exception("Failed to retrieve " + curDesc +
                            ": " + e.getMessage());
      }

      byte[] chunkData = rdm.getData();

      if (chunkData == null)
        throw new Exception("Unable to retrieve " + curDesc +
                            ": " + "Retrieve failed.");

      for (int i = 0; i < chunkData.length; ++i)
        fileData.add(new Byte(chunkData[i]));

      //System.arraycopy(chunkData, 0, fileData, offset, chunkData.length);

      if (chunkData.length < Chunk.maxSize)
      {
        System.out.println("Read trailing chunk.");
        break;
      }
      else
        offset += chunkData.length;

      curDesc = new ChunkDescriptor(f, offset);

      ghm = new GetHoldersMessage(clientID, controllerID, curDesc);

      try {
        ghm = ghm.send();
      }
      catch (Exception e) {
        throw new Exception("Failed to contact controller: " + e.getMessage());
      }

      holders = ghm.getChunkServers();
    }
    while (holders.length > 0);


    byte[] fileBytes = new byte[fileData.size()];
    for (int i = 0; i < fileBytes.length; ++i)
      fileBytes[i] = fileData.get(i).byteValue();

    if (f.exists())
      f.delete();

    f.createNewFile();

    FileOutputStream output = new FileOutputStream(f);

    output.write(fileBytes);

    // flush and close output stream
    output.flush();
    output.close();

    System.out.println("Successfully retrieved " + f + ".");
  }

  /**
   * Start a new client
   * @param args Command-line arguments
   */
  public static void main(String[] args)
  {
    int      contPort = 8000;          // controller port number
    String   contHost = "horde-0";     // controller host name
    String   fileName = "";            // name of file to process
    ModeKind mode     = ModeKind.None; // current mode of operation

    try
    {
      // for each command-line argument
      for (int i = 0; i < args.length; ++i)
      {
        // set to submit
        if (args[i].equals("--submit"))
        {
          if (mode != ModeKind.None)
            throw new Exception("Syntax error.");
          else
          {
            fileName = args[++i];
            mode = ModeKind.Submit;
          }
        }

        // set to receieve
        else if (args[i].equals("--retrieve"))
        {
          if (mode != ModeKind.None)
            throw new Exception("Syntax error.");
          else
          {
            fileName = args[++i];
            mode = ModeKind.Receive;
          }
        }

        // set controller port number
        else if (args[i].equals("--cont-port"))
          contPort = Integer.parseInt(args[++i]);

        // set controller host name
        else if (args[i].equals("--cont-host"))
          contHost = args[++i];

        // set name of file to process
        else if (args[i].equals("--file-name"))
          fileName = args[++i];

        // bad command line argument
        else
          throw new Exception("Bad argument.");
      }

      if (mode == ModeKind.None)
        throw new Exception("No mode of operation set.");
    }
    catch (Exception e)
    {
      System.out.println("Error processing command line arguments: " +
                         e.getMessage());
      System.exit(1);
    }

    try {
      // initialize new client
      Client clnt = new Client(new HostID(contHost, contPort));

      if (mode == ModeKind.Submit)
        // submit given file
        clnt.submitFile(new File(fileName));

      else if (mode == ModeKind.Receive)
        // retrieve given file
        clnt.retrieveFile(new File(fileName));

      else
        throw new Exception("Inappropriate mode of operation.");
    }
    catch (Exception e) {
      System.err.println("Error running client: " + e.getMessage() + " Aborting.");
      System.exit(1);
    }
  }
}
