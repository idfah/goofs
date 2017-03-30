package goofs;

import java.util.Random;

/**
 * Message requesting that data be stored
 * at given list of chunkservers and
 * associated with a given chunk descriptor.
 *
 * @author Elliott Forney
 */
public class SubmitDataMessage
  extends Message
{
  private static final long serialVersionUID = 1l;

  // random number generator
  private static Random rand = new Random();

  // index of current destination
  private int dstIndex = 0;

  // value added to current destination
  // index, this is randomized mod 3
  // so we hit different chunkservers when
  // sending the same message successively
  private int dstBase;

  // array of chunkservers to hold chunk
  private HostID[] chunkServers;

  // chunk descriptor to associate with data
  private ChunkDescriptor chunkDesc;

  // data for chunk to contain
  private byte[] data;

  /**
   * Create a new message requesting data storage.
   *
   * @param src Message source, client.
   * @param chunkServers Chunkservers to hold data.
   * @param chunkDesc Chunk descriptor describing data.
   * @param data Data to be held in destination chunk.
   */
  public SubmitDataMessage(HostID src, HostID[] chunkServers,
                           ChunkDescriptor chunkDesc, byte[] data)
  {
    // setup parent class, destination will be set below
    super(src, null, Message.Kind.SubmitData);

    this.chunkServers = chunkServers;

    // generate random starting index base
    dstBase = Math.abs(rand.nextInt()) % chunkServers.length;

    // set initial destination
    this.dst        = chunkServers[getDstIndex()];

    // set payload
    this.chunkDesc  = chunkDesc;
    this.data       = data;
  }

  // generate a new destination index relative to dstBase
  private int getDstIndex()
  {
    return ((dstBase+dstIndex) % chunkServers.length);
  }

  /**
   * Set message to be sent to next chunkserver.
   *
   * @return True if there are more destinations,
   *   false otherwise.
   */
  public boolean setNextDest()
  {
    if (++dstIndex < chunkServers.length)
    {
      dst = chunkServers[getDstIndex()];
      return true;
    }
    else
    {
      dst = null;
      return false;
    }
  }

  /**
   *
   */ 
  public ChunkDescriptor getChunkDescriptor()
  {
    return chunkDesc;
  }

  /**
   *
   */
  public byte[] getData()
  {
    return data;
  }

  /**
   *
   */
  public SubmitDataMessage send()
    throws Exception
  {
    SubmitDataMessage reply = null;
    boolean success = false;

    while (!success && (dst != null))
    {
      try {
        System.out.println("Submitting data " + chunkDesc + " to " + dst);
        reply = (SubmitDataMessage)super.send();
        success = true;
      }
      catch (Exception e) {
        System.out.println("Failed to submit data to " + dst +
                           ": " + e.getMessage());
        setNextDest();
      }
    }

    if (success == false)
      throw new Exception("All forward destinations unavailable.");

    return reply;
  }
}
