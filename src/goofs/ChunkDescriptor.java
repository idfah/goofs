package goofs;

import java.io.*;

/**
 *
 * @author Elliott Forney
 */
public class ChunkDescriptor
  implements Serializable
{
  private static final long serialVersionUID = 1l;

  //
  private File f;

  //
  private int offset;

  /**
   *
   */
  public ChunkDescriptor(File f, int offset)
  {
    this.f = f;
    this.offset = offset;
  }

  /**
   *
   */
  public File getFile()
  {
    return f;
  }

  public int getOffset()
  {
    return offset;
  }

  /**
   *
   */
  public String toString()
  {
    return f + ":" + (offset/Chunk.maxSize) + "/" + offset;
  }

  /**
   *
   */
  public String toSeqString()
  {
    return f + ":" + (offset/Chunk.maxSize);
  }

  /**
   *
   */
  public boolean equals(ChunkDescriptor cd)
  {
    if ( (f.toString().equals(cd.f.toString())) &&
         (offset/Chunk.maxSize) == (cd.offset/Chunk.maxSize) )
      return true;
    else
      return false;
  }
}
