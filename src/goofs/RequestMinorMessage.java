package goofs;

/**
 * Request that a minor heartbeat be sent to controller.
 *
 * @author Elliott Forney
 */
public class RequestMinorMessage
  extends Message
{
  private static final long serialVersionUID = 1l;

  /**
   *
   */
  public RequestMinorMessage(HostID src, HostID dst)
  {
    super(src, dst, Message.Kind.RequestMinor);
  }

  /**
   *
   */
  public RequestMinorMessage send()
    throws Exception
  {
    return (RequestMinorMessage)super.send();
  }
}
