package goofs;

/**
 * Message requesting chunkservers
 * that are currently holding replicas
 * associated with a given chunk
 * descriptor.
 *
 * @author Elliott Forney
 */
public class GetHoldersMessage
  extends Message
{
  private static final long serialVersionUID = 1l;

  // descriptor for chunk we are concerned with
  private ChunkDescriptor chunkDesc;

  // chunkservers holding chunk we are concerned with
  private HostID[] chunkServers = null;

  /**
   * Send message requesting chunkservers
   * holding a chunk associated with a
   * given chunk descriptor.
   *
   * @param src Message source, client or chunkserver.
   * @param dst Message destination, controller.
   * @param chunkDesc Chunk descriptor associated with
   *   the chunk we are interested in.
   */
  public GetHoldersMessage(HostID src, HostID dst, ChunkDescriptor chunkDesc)
  {
    super(src, dst, Message.Kind.GetHolders);
    this.chunkDesc = chunkDesc;
  }

  /**
   *
   */
  public boolean expectsReply()
  {
    return true;
  }

  /**
   * Get the chunk descriptor sent in this message.
   *
   * @return The chunk descriptor currently
   *   stored in this message.
   */ 
  public ChunkDescriptor getChunkDescriptor()
  {
    return chunkDesc;
  }

  /**
   * Set the chunk servers sent in this message.
   *
   * @param The host id's of the chunk servers to
   *   send in this message.
   */
  public void setChunkServers(HostID[] chunkServers)
  {
    this.chunkServers = chunkServers;
  }

  /**
   * Get the chunk servers sent in this message.
   *
   * @return The chunk servers currently
   *   stored in this message.
   */
  public HostID[] getChunkServers()
  {
    return chunkServers;
  }

  /**
   * Send this message to its
   * current destination.
   *
   * @return Reply message.
   */ 
  public GetHoldersMessage send()
    throws Exception
  {
    return (GetHoldersMessage)super.send();
  }
}
