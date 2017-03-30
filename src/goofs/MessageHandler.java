package goofs;

import java.io.*;
import java.lang.Thread;
import java.net.*;

/**
 * Abstract message handler for messages
 * in goofs filesystem.  handle method
 * must be defined in subclass.
 *
 * @author Elliott Forney
 */
public abstract class MessageHandler
  extends Thread
{
  private Socket s; // socket handling the request

  /**
   *  Create new message handler.
   * @param s Socket used to get message and send reply
   */
  public MessageHandler(Socket s)
  {
    this.s = s;
  }

  /**
   * Start a new message handler thread.
   * Sets up object streams on socket,
   * gets message, calls handle and 
   * sends reply.
   */
  public void run()
  {
    ObjectInputStream  input  = null; // object input stream on socket
    ObjectOutputStream output = null; // object output stream on socket

    try
    {
      // open new object input stream on socket
      input =
        new ObjectInputStream(s.getInputStream());

      // get message from input
      Message got = (Message)input.readObject();

      // System.out.println("recv: " + got);

      // call handler to process message
      Message put = handle(got);

      if ((put != null) && (got.expectsReply()))
      {
        // open new object output stream on socket
        output =
          new ObjectOutputStream(s.getOutputStream());

        // System.out.println("send: " + put);

        // send reply message
        output.writeObject(put);

        // flush and close output stream
        output.flush();
        output.close();
      }
    }
    catch (Exception e) {
      System.err.println("Error processing message: " + e.getMessage());
    }

    try {
    // close input stream
    input.close();

    // close socket
    s.close();
    }
    catch (Exception e) {
      // do nothing
    }
  }

  /**
   * Handle incomming message and generate reply message.
   * @param m Incomming message
   * @return Reply message
   */
  public abstract Message handle(Message m)
    throws Exception;
}
