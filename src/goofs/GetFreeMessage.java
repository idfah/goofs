package goofs;

/**
 * Message requesting chunkservers
 * that are eligible to store new
 * chunks.
 *
 * @author Elliott Forney
 */
public class GetFreeMessage
  extends Message
{
  private static final long serialVersionUID = 1l;

  // chunkservers to hold new chunks
  private HostID[] chunkServers= null;

  /**
   * Create new message requesting chunkservers
   * that are eligible to store new chunks.
   *
   * @param src Message source, a client.
   * @param dst Message destination, a controller.
   */
  public GetFreeMessage(HostID src, HostID dst)
  {
    super(src, dst, Message.Kind.GetFree);
  }

  /**
   *
   */
  public boolean expectsReply()
  {
    return true;
  }

  /**
   * Set the chunkservers to send.
   *
   * @param The chunkservers to
   *   send in this message.
   */
  public void setChunkServers(HostID[] chunkServers)
  {
    this.chunkServers = chunkServers;
  }

  /**
   * Get the chunkservers sent.
   *
   * @return Array of chunkservers
   *   sent in this message.
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
  public GetFreeMessage send()
    throws Exception
  {
    return (GetFreeMessage)super.send();
  }
}
