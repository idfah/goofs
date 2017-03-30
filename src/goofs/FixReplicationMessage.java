package goofs;

import java.util.Random;

/**
 *
 * @author Elliott Forney
 */
public class FixReplicationMessage
  extends Message
{
  private static final long serialVersionUID = 1l;

  private ChunkDescriptor chunkDesc;

  private HostID newHolder;

  /**
   *
   */
  public FixReplicationMessage(HostID src, HostID dst, HostID newHolder,
                               ChunkDescriptor chunkDesc)
  {
    super(src, dst, Message.Kind.FixReplication);
    this.newHolder = newHolder;
    this.chunkDesc = chunkDesc;
  }

  /**
   *
   */ 
  public ChunkDescriptor getChunkDescriptor()
  {
    return chunkDesc;
  }

  public HostID getNewHolder()
  {
    return newHolder;
  }

  /**
   *
   */
  public FixReplicationMessage send()
    throws Exception
  {
    return (FixReplicationMessage)super.send();
  }
}
