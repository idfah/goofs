package goofs;

import java.util.Random;

/**
 *
 * @author Elliott Forney
 */
public class FixCorruptionMessage
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

  // array of chunkservers holding data
  private HostID[] chunkServers;

  // chunk descriptor to associate with data
  private ChunkDescriptor chunkDesc;

  // data for chunk to contain
  private Chunk chunk = null;

  /**
   *
   */
  public FixCorruptionMessage(HostID src, HostID[] chunkServers,
                              ChunkDescriptor chunkDesc)
  {
    // setup parent class, destination will be set below
    super(src, null, Message.Kind.FixCorruption);

    this.chunkServers = chunkServers;
    this.chunkDesc    = chunkDesc;

    // generate random starting index base
    dstBase = Math.abs(rand.nextInt()) % chunkServers.length;

    // set initial destination
    this.dst = chunkServers[getDstIndex()];
  }

  /**
   *
   */
  public boolean expectsReply()
  {
    return true;
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
  public Chunk getChunk()
  {
    return chunk;
  }

  /**
   *
   */
  public void setChunk(Chunk chunk)
  {
    this.chunk = chunk;
  }

  /**
   *
   */
  public FixCorruptionMessage send()
    throws Exception
  {
    FixCorruptionMessage reply = null;
    boolean success = false;

    while (!success && (dst != null))
    {
      try {
        System.out.println("Retrieving data " + chunkDesc + " from " + dst);
        reply = (FixCorruptionMessage)super.send();
        success = true;
      }
      catch (Exception e) {
        System.out.println("Failed to retrieve data from " + dst +
                           ": " + e.getMessage());
        setNextDest();
      }
    }

    if (success == false)
      throw new Exception("All destinations unavailable.");

    return reply;
  }
}
