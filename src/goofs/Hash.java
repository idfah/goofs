package goofs;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *
 */
public class Hash
  implements Serializable
{
  private static final long serialVersionUID = 1l;

  //
  private BigInteger hsh;

  /**
   *
   */
  public Hash(String hex)
  {
    this.hsh = hexToBigInteger(hex);
  }

  /**
   *
   */
  public Hash(BigInteger bi)
  {
    this.hsh = bi.abs();
  }

  /**
   *
   */
  public Hash(byte[] data)
    throws Exception
  {
    try
    {
      MessageDigest md = MessageDigest.getInstance("SHA1");
      md.update(data);
      hsh = new BigInteger(1, md.digest());
    }
    catch (Exception e) {
      throw new Exception("Failed to generate hash: " + e.getMessage());
    }
  }

  /**
   *
   */
  public BigInteger getHash()
  {
    return hsh;
  }

  /**
   *
   */
  public String asHex()
  {
    return bigIntegerToHex(hsh);
  }

  /**
   *
   */
  public String toString()
  {
    return asHex();
  }

  /**
   *
   */
  public boolean equals(Hash h)
  {
    return hsh.equals(h.hsh);
  }

  /**
   *
   */
  public static BigInteger hexToBigInteger(String hex)
  {
    // following by Shrideep Pallickara
    int size = hex.length();
    byte[] buf = new byte[size / 2];
    int j = 0;
    for (int i = 0; i < size; i++)
    {
      String a = hex.substring(i, i + 2);
      int valA = Integer.parseInt(a, 16);
      i++;
      buf[j] = (byte)valA;
      j++;
    }
    // end by Shrideep

    return (new BigInteger(1, buf));
  }

  /**
   *
   */
  public static String bigIntegerToHex(BigInteger bi)
  {
    return String.format("%1$040X", bi);
  }
}
