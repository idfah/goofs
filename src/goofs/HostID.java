package goofs;

import java.io.*;
import java.lang.String;
import java.net.*;

/**
 * Node identification.
 *
 * @author Elliott Forney
 */
public class HostID
  implements Serializable
{
  private static final long serialVersionUID = 1l;

  private String host;
  private int    port;

  /**
   * Create a new node identifier.
   *
   * @param host Host name.
   * @param port Port number.
   */
  public HostID(String host, int port)
    throws Exception
  {
    // if host name is null
    // or empty string
    if ( (host == null) ||
         (host == "")   )
      // get local host name
      getHostName();

    else
      this.host = host;

    this.port = port;
  }

  /**
   * Create a new node identifier.
   * Use local host name.
   *
   * @param port Port numer.
   */
  public HostID(int port)
    throws Exception
  {
    // get local host name
    this.host = getHostName();

    this.port = port;
  }

  // get local host name
  private String getHostName()
    throws Exception
  {
    String h;

    try {
      h = InetAddress.getLocalHost().getHostName();
    }
    catch (Exception e) {
      throw new Exception("Error getting local hostname: " + e.getMessage());
    }

    return h;
  }

  /**
   * Get port number for this node identifier.
   *
   * @return Port number for this node identifier.
   */
  public int getPort()
  {
    return port;
  }

  /**
   * Get host name for this node identifier.
   *
   * @return Host name for this node identifier.
   */
  public String getHost()
  {
    return host;
  }

  /**
   * Check for equality with another node identifier.
   * 
   * @return True if host names and port numbers match.
   *   False otherwise.
   */
  public boolean equals(HostID id)
  {
    if (host.equals(id.host) &&
        (port == id.port)    )
      return true;
    else
      return false;
  }

  /**
   * Convert this node identifier to a
   * human readable string.
   *
   * @return A human readable string describing
   *   this node identifier.
   */
  public String toString()
  {
    return new String(host + ":" + port);
  }
}
