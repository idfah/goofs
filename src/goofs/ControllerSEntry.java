package goofs;

import java.lang.Comparable;
import java.util.Collection;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;

/**
 *
 * @author Elliott Forney
 */
public class ControllerSEntry
  implements Comparable<ControllerSEntry>
{
  /** */
  public static final long expirationSecs = 2*ChunkServer.minorHeartbeatSecs;

  // chunks replicated at chunkserver
  // by chunk descriptor string
  private Hashtable<String, ChunkDescriptor> chunks =
    new Hashtable<String, ChunkDescriptor>();

  // identification for chunkserver
  private HostID id;

  // time that this entry was last acknowledged
  private long timeStamp;

  // approximate number of chunks currently stored
  private int numChunks = 0;

  /**
   * Create new entry for list of chunk servers.
   * Number of chunks sored is set to zero.
   *
   * @param id Identification for chunkserver we are describing.
   * @param chunkDesc Chunks initially on chunkserver.
   */
  public ControllerSEntry(HostID id, ChunkDescriptor[] chunkDesc)
  {
    this.id = id;
    this.timeStamp = (new Date()).getTime();

    for (int i = 0; i < chunkDesc.length; ++i)
      chunks.put(chunkDesc[i].toSeqString(), chunkDesc[i]);
  }

  /**
   * Get the host id of the chunkserver
   * described by this entry.
   *
   * @return The host id of the chunkserver
   *   described by this entry.
   */
  public HostID getID()
  {
    return id;
  }

  /**
   *
   */
  public synchronized void setNumChunks(int n)
  {
    numChunks = n;
  }

  /**
   *
   */
  public synchronized void incrementNumChunks()
  {
    ++numChunks;
  }

  /**
   *
   */
  public synchronized void touch(ChunkDescriptor[] chunkDesc)
  {
    // update timestamp to current time
    timeStamp = (new Date()).getTime();

    synchronized (chunks)
    {
      for (int i = 0; i < chunkDesc.length; ++i)
      {
        ChunkDescriptor curDesc =
          chunks.get(chunkDesc[i].toSeqString());

        if (curDesc == null)
          chunks.put(chunkDesc[i].toSeqString(), chunkDesc[i]);
      }
    }
  }

  /**
   *
   */
  public boolean hasExpired()
  {
    long curTime = (new Date()).getTime();

    if ((curTime - timeStamp) > (expirationSecs*1000))
      return true;
    else
      return false;
  }

  /**
   *
   */
  public boolean holdsChunk(ChunkDescriptor cd)
  {
    boolean found;

    synchronized (chunks) {
      found = chunks.containsKey(cd.toString());
    }

    return found;
  }

  /**
   *
   */
  public ChunkDescriptor[] getChunks()
  {
    ChunkDescriptor[] chunkDesc;

    synchronized (chunks)
    {
      //
      chunkDesc =
        new ChunkDescriptor[chunks.size()];

      //
      Iterator<ChunkDescriptor> itr =
        chunks.values().iterator();

      //
      int i = 0;
      while (itr.hasNext())
        //
        chunkDesc[i++] = itr.next();
    }

    return chunkDesc;
  }

  /**
   * Generate human readable string describing this chunkserver list entry.
   *
   * @return Human readable string describing this chunkserver list entry.
   */
  public String toString()
  {
    return id + " " + numChunks + " " + timeStamp;
  }

  /**
   *
   */
  public boolean equals(ControllerSEntry cme)
  {
    if (id.equals(cme.id))
      return true;
    else
      return false;
  }

  /**
   *
   */
  public boolean equals(HostID cs)
  {
    if (id.equals(cs))
      return true;
    else
      return false;
  }

  /**
   * Compare this entry to another by number of chunks stored.
   *
   * @return -1 if number of chunks stored in this entry
   *   is less than in csle.  0 if they are the same.
   *   1 if if number of chunks stored in this entry is
   *   greater than in csle.
   */
  public int compareTo(ControllerSEntry csle)
  {
    if (numChunks < csle.numChunks)
      return -1;
    else if (numChunks == csle.numChunks)
      return 0;
    else
      return 1;
  }
}
