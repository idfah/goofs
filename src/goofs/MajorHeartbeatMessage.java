package goofs;

/**
 * Major heartbeat from chunkserver to controller.
 *
 * @author Elliott Forney
 */
public class MajorHeartbeatMessage
  extends Message
{
  private static final long serialVersionUID = 1l;

  // descriptors for all chunks
  private ChunkDescriptor[] allChunks;

  //
  private ChunkDescriptor[] corruptChunks;

  /**
   *  Create new major heartbeat message
   *
   * @param src Message source.
   * @param dst Message destination.
   */
  public MajorHeartbeatMessage(HostID src, HostID dst,
                               ChunkDescriptor[] allChunks,
                               ChunkDescriptor[] corruptChunks)
  {
    super(src, dst, Message.Kind.MajorHeartbeat);
    this.allChunks      = allChunks;
    this.corruptChunks  = corruptChunks;
  }

  /**
   *
   */
  public ChunkDescriptor[] getAllChunks()
  {
    return allChunks;
  }

  /**
   *
   */
  public ChunkDescriptor[] getCorruptChunks()
  {
    return corruptChunks;
  }

  /**
   *
   */
  public MajorHeartbeatMessage send()
    throws Exception
  {
    return (MajorHeartbeatMessage)super.send();
  }
}
