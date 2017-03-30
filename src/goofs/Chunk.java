package goofs;

import java.io.*;
import java.lang.String;
import java.lang.Integer;
import java.lang.Long;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;

/**
 * Chunk, or piece, of a file
 * in goofs filesystem.
 *
 * @author Elliott Forney
 */
public class Chunk
  implements Serializable
{
  /** maximum bytes in a single chunk */
  public static final int maxSize = 64*1024; // 64KB

  /** size of checksum slice */
  public static final int checkSize = 8*1024; // 8KB

  private static final long serialVersionUID = 1l;

  // abstract pathname for the file
  // that this chunk belongs to
  private File f;

  // chunk number within the file
  private int sequence;

  // file version associated with this chunk
  private int version = 0;

  // last-modified time for this chunk
  // in seconds since epoch
  private long timeStamp;

  // bytes in this chunk
  private byte[] data;

  // checksum for each slice
  private Hash[] checkSums;

  /**
   * Create a chunk containing a piece of a file
   * @param f Abstract pathname of file containing this chunk
   * @param data Bytes belonging to this chunk
   * @param sequence Chunk number within the file
   */
  public Chunk(File f, int sequence, byte[] data)
    throws Exception
  {
    // throw exception if data is too big
    if (data.length > maxSize)
      throw new Exception("Chunk size " + data.length +
                          " too big, max is " + maxSize + ".");

    this.f         = f;
    this.sequence  = sequence;
    this.timeStamp = (new Date()).getTime();
    this.data      = data;

    // generate checksums
    genCheckSums();
  }

  /**
   *
   */ 
  public Chunk(ChunkDescriptor chunkDesc, byte[] data)
    throws Exception
  {
    // throw exception if data is too big
    if (data.length > maxSize)
      throw new Exception("Chunk size " + data.length +
                          " too big, max is " + maxSize + ".");

    this.f         = chunkDesc.getFile();
    this.sequence  = chunkDesc.getOffset()/maxSize;
    this.timeStamp = (new Date()).getTime();
    this.data      = data;

    // generate checksums
    genCheckSums();
  }

  /**
   *
   */
  public Chunk(File f, int sequence, int version, long timeStamp,
               Hash[] checkSums, byte[] data)
  {
    this.f          = f;
    this.sequence   = sequence;
    this.version    = version;
    this.timeStamp  = timeStamp;
    this.checkSums  = checkSums;
    this.data       = data;
  }

  /**
   *
   */
  public File getFile()
  {
    return f;
  }

  /**
   *
   */
  public int getSequence()
  {
    return sequence;
  }
    
  /**
   *
   */
  public int getVersion()
  {
    return version;
  }
    
  /**
   *
   */
  public long getTimeStamp()
  {
    return timeStamp;
  }

  /**
   * Get data stored by this chunk
   */
  public byte[] getData()
  {
    return data;
  }

  /**
   *
   */
  public void setData(byte[] data)
    throws Exception
  {
    ++(this.version);
    this.timeStamp = (new Date()).getTime();
    this.data      = data;

    // generate checksums
    genCheckSums();
  }

  /**
   *
   */
  public static void delete(ChunkDescriptor chunkDesc)
  {
    String fileName = chunkDesc.getFile().toString();
    int sequenceNum = chunkDesc.getOffset()/maxSize;

    File metaf = new File("/tmp/idfah-goofs/" +
                          fileName + "_meta"  +
                          sequenceNum);

    File dataf = new File("/tmp/idfah-goofs/" +
                          fileName + "_data"  +
                          sequenceNum);

    // if metadata exists, delete it
    if (metaf.exists())
      metaf.delete();

    if (dataf.exists())
      dataf.delete();
  }

  /**
   * Read chunk from disk.
   */
  public static Chunk read(ChunkDescriptor cd)
    throws Exception
  {
    File    f         = cd.getFile();
    int     sequence  = cd.getOffset()/maxSize;
    int     version   = 0;
    long    timeStamp = 0;
    ArrayList<Hash> runningCheckSums= new ArrayList<Hash>();
    Hash[]  checkSums = null;
    byte[]  data      = null;

    // file holding meta-data
    File metaSrc = new File("/tmp/idfah-goofs/" +
                            //f.getAbsoluteFile().toString()
                            f.toString() +
                            "_meta" + sequence);

    // complain if file doesn't exist
    if (!metaSrc.isFile())
      throw new Exception("Can't read " + metaSrc + ": does not exist.");

    System.out.println("Reading: " + metaSrc.getAbsoluteFile());

    // open buffered reader on metadata
    BufferedReader metaInput =
      new BufferedReader(
        new InputStreamReader(
          new FileInputStream(metaSrc)));

    int lineNo = 0; // current line number
    try
    {
      //
      String currentLine = metaInput.readLine();

      while (currentLine != null)
      {
        ++lineNo;

        StringTokenizer separator =
          new StringTokenizer(currentLine);

        if (separator.hasMoreTokens() == true)
        {
          String keyword  = separator.nextToken();

          if (keyword.equals("file")) {
            // do nothing
          }

          else if (keyword.equals("sequence")) {
            // do nothing
          }

          else if (keyword.equals("version"))
            version = Integer.parseInt(separator.nextToken());

          else if (keyword.equals("timestamp"))
            timeStamp = Long.parseLong(separator.nextToken());

          else if (keyword.equals("checksum"))
          {
            int index = Integer.parseInt(separator.nextToken());
            runningCheckSums.add(new Hash(separator.nextToken()));
          }
          else
            throw new Exception("Failed to read meatadata " +
                                metaSrc.getAbsoluteFile()   +
                                " at line " + lineNo        +
                                ": Unknown keyword.");
        }

        currentLine = metaInput.readLine();
      }
    }
    catch (Exception e) {
      throw new Exception("Failed to read meatadata " +
                          metaSrc.getAbsoluteFile()   +
                          " at line " + lineNo        +
                          ": " + e.getMessage());
    }

    //
    metaInput.close();

    //
    checkSums = runningCheckSums.toArray(new Hash[0]);

    // file holding data
    File dataSrc = new File("/tmp/idfah-goofs/" +
                            //f.getAbsoluteFile().toString()
                            f.toString() +
                           "_data" + sequence);

    // complain if file doesn't exist
    if (!dataSrc.isFile())
      throw new Exception("Can't read " + dataSrc + ": does not exist.");

    System.out.println("Reading: " + dataSrc.getAbsoluteFile());

    InputStream dataInput = new FileInputStream(dataSrc);

    byte[] curData   = new byte[maxSize];
    int numBytesRead = dataInput.read(curData, 0, maxSize);
    if (numBytesRead <= 0)
      throw new Exception("Error reading " + dataSrc + ": Empty File.");

    data = new byte[numBytesRead];

    /*for (int i = 0; i < numBytesRead; ++i)
      data[i] = curData[i]; */
    System.arraycopy(curData, 0, data, 0, numBytesRead);

    dataInput.close(); 

    checkCheckSums(checkSums, data);

    return new Chunk(f, sequence, version,
                     timeStamp, checkSums, data);
  }

  /**
  * Write chunk to disk.
  */
  public void write()
    throws Exception
  {
    File metaDst = new File("/tmp/idfah-goofs/" +
                            //f.getFile().getAbsoluteFile().toString() +
                            f.toString() +
                            "_meta" + sequence);

    // if metadata already exists, delete it
    if (metaDst.exists())
      metaDst.delete();

    // create parent directories if they don't exist
    File parentDir = metaDst.getParentFile();
    if (parentDir != null)
      parentDir.mkdirs();

    // open buffered writer for metadata
    BufferedWriter metaOutput =
      new BufferedWriter(
        new FileWriter(metaDst));

    metaOutput.write("file " + f.toString());
    metaOutput.newLine();

    metaOutput.write("sequence " + sequence);
    metaOutput.newLine();

    metaOutput.write("version " + version);
    metaOutput.newLine();

    metaOutput.write("timestamp " + timeStamp);
    metaOutput.newLine();

    for (int i = 0; i < checkSums.length; ++i)
    {
      metaOutput.write("checksum " + i + " " + checkSums[i]);
      metaOutput.newLine();
    }

    // flush and close output stream
    metaOutput.flush();
    metaOutput.close();

    File dataDst = new File("/tmp/idfah-goofs/" +
                            //f.getFile().getAbsoluteFile().toString() +
                            f.toString() +
                            "_data" + sequence);

    System.out.println("Writing: " + dataDst.getAbsoluteFile());

    // if data already exists, delete it
    if (dataDst.exists())
      dataDst.delete();

    // create parent directories if they don't exist
    parentDir = dataDst.getParentFile();
    if (parentDir != null)
      parentDir.mkdirs();

    // create new chunk file
    dataDst.createNewFile();

    //
    FileOutputStream dataOutput = new FileOutputStream(dataDst);

    // write the chunk
    dataOutput.write(data);

    // flush and close output stream
    dataOutput.flush();
    dataOutput.close();
  }

  /**
   *
   */
  public String toString()
  {
    return "Name: "      + f            + ",\t" +
           "Sequence: "  + sequence     + ",\t" +
           "Version: "   + version      + ",\t" +
           "Timestamp: " + timeStamp;
  }

  // generate checksums for each slice
  private void genCheckSums()
    throws Exception
  {
    int numChecks = data.length/checkSize;
    int lastCheckSize = data.length%checkSize;

    if (lastCheckSize > 0)
      ++numChecks;

    checkSums = new Hash[numChecks];

    try
    {
      byte[] hashData;
      int    curByte;
      for (curByte = 0; curByte < numChecks-1; ++curByte)
      {
        hashData = new byte[checkSize];
        for (int i = 0; i < checkSize;  ++i)
          hashData[i] = data[curByte+i];

        checkSums[curByte] = new Hash(hashData);
      }

      hashData = new byte[lastCheckSize];
      for (int i = 0; i < lastCheckSize; ++i)
        hashData[i] = data[curByte+i];

      checkSums[numChecks-1] = new Hash(hashData);
    }
    catch (Exception e) {
      throw new Exception("Error generating checksum.");
    }
  }

  //
  private static void checkCheckSums(Hash[] checkSums, byte[] data)
    throws Exception
  {
    int numChecks = data.length/checkSize;
    int lastCheckSize = data.length%checkSize;

    if (lastCheckSize > 0)
      ++numChecks;

    Hash[] newSums = new Hash[numChecks];

    try
    {
      byte[] hashData;
      int    curByte;
      for (curByte = 0; curByte < numChecks-1; ++curByte)
      {
        hashData = new byte[checkSize];
        for (int i = 0; i < checkSize;  ++i)
          hashData[i] = data[curByte+i];

        newSums[curByte] = new Hash(hashData);
      }

      hashData = new byte[lastCheckSize];
      for (int i = 0; i < lastCheckSize; ++i)
        hashData[i] = data[curByte+i];

      newSums[numChecks-1] = new Hash(hashData);
    }
    catch (Exception e) {
      throw new Exception("Error generating checksum.");
    }

    for (int i = 0; i < checkSums.length; ++i)
      if (!newSums[i].equals(checkSums[i]))
        throw new Exception("Corruption detected at slice " +
                            i + ".");
  }
}
