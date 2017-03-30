package goofs;

/**
 * Minor heartbeat from chunkserver to controller.
 *
 * @author Elliott Forney
 */
public class MinorHeartbeatMessage
  extends Message
{
  private static final long serialVersionUID = 1l;

  // descriptors for chunks added since last heartbeat
  private ChunkDescriptor[] newChunks;

  //
  private ChunkDescriptor[] corruptChunks;

  /**
   *  Create new minor heartbeat message
   *
   * @param src Message source.
   * @param dst Message destination.
   * @param newChunks Chunks added since last heartbeat
   */
  public MinorHeartbeatMessage(HostID src, HostID dst,
                               ChunkDescriptor[] newChunks,
                               ChunkDescriptor[] corruptChunks)
  {
    super(src, dst, Message.Kind.MinorHeartbeat);
    this.newChunks     = newChunks;
    this.corruptChunks = corruptChunks;
  }

  /**
   *
   */
  public ChunkDescriptor[] getNewChunks()
  {
    return newChunks;
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
  public MinorHeartbeatMessage send()
    throws Exception
  {
    return (MinorHeartbeatMessage)super.send();
  }
}
