package goofs;

import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;

/**
 * Generic goofs message.
 *
 * @author Elliott Forney
 */
public class Message
  implements Serializable
{
  private static final long serialVersionUID = 1l;

  /**
   * Kinds of messages that can be sent in goofs.
   */
  public static enum Kind {
    MinorHeartbeat, MajorHeartbeat,
    RequestMinor,   RequestMajor,
    GetFree,        GetHolders,
    SubmitData,     RetrieveData,
    FixReplication, FixCorruption
  }

  /** This message's kind */
  public Kind kind;

  protected HostID src; // message source id
  protected HostID dst; // message destination id

  /**
   *  Create a new goofs message.
   *
   * @param src Message source.
   * @param dst Message destination.
   * @param kind Message kind.
   */
  public Message(HostID src, HostID dst, Kind kind)
  {
    this.src  = src;
    this.dst  = dst;
    this.kind = kind;
  }

  /**
   * Get message source id.
   */
  public HostID getSource()
  {
    return src;
  }

  /**
   * Set message source id.
   *
   * @param src New message source.
   */
  public void setSource(HostID src)
  {
    this.src = src;
  }

  /**
   * Get message destination id.
   */
  public HostID getDest()
  {
    return dst;
  }

  /**
   * Set message destination id.
   *
   * @param dst New Message destination.
   */
  public void setDest(HostID dst)
  {
    this.dst = dst;
  }

  /**
   * Swap message source and destination
   */
  public void swapSrcDst()
  {
    HostID newDst = src;
    src = dst;
    dst = newDst;
  }

  /**
   * Determine whether or not this message
   * should attempt to get a reply.
   *
   * @return True if this message should
   *   wait for a reply after it is sent
   *   and false otherwise.
   */
  public boolean expectsReply()
  {
    return false;
  }

  /**
   * Send this message to its destination and get reply.
   *
   * @return Message reply.
   */
  public Message send()
    throws Exception
  {
    // object stream for sending message
    ObjectOutputStream output = null;

    // outgoing socket
    Socket s = null;

    // message reply received
    Message reply = null;

    // try and send message
    try
    {
      // open socket to destination
      s = new Socket(dst.getHost(), dst.getPort());

      // open output stream on socket
      output =
        new ObjectOutputStream(s.getOutputStream());

      // send the message
      output.writeObject(this);

      // flush and close output stream
      output.flush();
    }
    catch (Exception e) {
      throw new Exception("Error sending message " + this +
                          ": " + e.getMessage());
    }

    if (expectsReply())
    {
      // object stream for receiving reply
      ObjectInputStream input = null;

      // try and get reply
      try
      {
        // open input stream on socket
        input =
          new ObjectInputStream(s.getInputStream());

        // get message reply
        reply = (Message)input.readObject();

        // close input stream
        input.close();
      }
      catch(Exception e) {
        throw new Exception("Error getting message reply " + this +
                            ": " + e.getMessage());
      }
    }

    try {
      // close output stream
      output.close();

      // close socket
      s.close();
    }
    catch (Exception e) {
      // do nothing
    }

    return reply;
  }

  /**
   * Convert message to a human readable string.
   *
   * @return Human readible string representing this message.
   */
  public String toString()
  {
    return (src + "->" + dst + "#" + kind);
  }
}
