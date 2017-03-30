package goofs;

/**
 * Request that a major heartbeat be sent to controller.
 *
 * @author Elliott Forney
 */
public class RequestMajorMessage
  extends Message
{
  private static final long serialVersionUID = 1l;

  /**
   *
   */
  public RequestMajorMessage(HostID src, HostID dst)
  {
    super(src, dst, Message.Kind.RequestMajor);
  }

  /**
   *
   */
  public RequestMajorMessage send()
    throws Exception
  {
    return (RequestMajorMessage)super.send();
  }
}
