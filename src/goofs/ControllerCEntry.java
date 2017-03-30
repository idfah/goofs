package goofs;

import java.io.*;
import java.util.ArrayList;
import java.util.ListIterator;

/**
 * Entry in controller map describing the
 * servers that replicate a given chunk.
 *
 * @author Elliott Forney
 */
public class ControllerCEntry
{
  // chunk descriptor for this entry
  private ChunkDescriptor chunkDesc;

  // list of chunkservers replicating
  // the chunk described by this entry
  private ArrayList<HostID> chunkServers =
    new ArrayList<HostID>();

  /**
   * Create a new chunk entry for a given chunk descriptor.
   */
  public ControllerCEntry(ChunkDescriptor chunkDesc, HostID cs)
  {
    this.chunkDesc = chunkDesc;
    chunkServers.add(cs);
  }

  /**
   *
   */
  public ChunkDescriptor getDescriptor()
  {
    return chunkDesc;
  }

  /**
   * Get chunkservers currently replicating this chunk.
   *
   * @return An array of id's for hosts currently
   *   replicating this chunk.
   */
  public HostID[] getChunkServers()
  {
    synchronized (chunkServers)
    {
      return chunkServers.toArray(new HostID[0]);
    }
  }

  /**
   * Get the number of chunkservers currently
   * replicating this chunk.
   *
   * @return The number of chunkservers currently
   *   replicating this chunk.
   */
  public int getNumChunkServers()
  {
    synchronized (chunkServers)
    {
      return chunkServers.size();
    }
  }

  /**
   *
   */
  public void touch(HostID cs)
  {
    synchronized (chunkServers)
    {
      boolean found = false;
      for (int i = 0; i < chunkServers.size(); ++i)
        if (chunkServers.get(i).equals(cs))
        {
          found = true;
          break;
        }

      if (found == false)
        chunkServers.add(cs);
    }
  }

  /**
   * Add a new chunkserver that
   * is now replicating this chunk.
   *
   * @param cs A chunkserver that is
   *   now replicating this chunk.
   */
  public void addChunkServer(HostID cs)
  {
    synchronized (chunkServers)
    {
      chunkServers.add(cs);
    }
  }

  /**
   * Remove a chunkserver from the list
   * of chunkservers currently replicating
   * this chunk.
   *
   * @param cs The id of the chunkserver to remove.
   */
  public void delChunkServer(HostID cs)
  {
    synchronized (chunkServers)
    {
      // open iterator on replica list
      ListIterator<HostID> itr =
        chunkServers.listIterator();

      // until end of list
      while (itr.hasNext())
      {
        // get next replica
        HostID serv = itr.next();

        // if this is the replica
        // marked for deletion
        if (serv.equals(cs))
          itr.remove();
      }
    }
  }

  /**
   * Set the list of chunkservers
   * currently replicating this chunk.
   *
   * @param cs List of chunkservers
   *   currently replicating this chunk.
   */
  public void setChunkServers(HostID[] cs)
  {
    synchronized (chunkServers)
    {
      chunkServers.clear();
      for (HostID serv : cs)
        chunkServers.add(serv);
    }
  }

  /**
   *
   */
  public String toString()
  {
    String s = "";

    synchronized (chunkServers)
    {
      s += chunkDesc.toString();

      for (int i = 0; i < chunkServers.size(); ++i)
        s += " -> " + chunkServers.get(i);
    }

    return s;
  }
}
