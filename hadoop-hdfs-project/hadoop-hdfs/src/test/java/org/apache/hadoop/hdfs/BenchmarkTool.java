package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.File;
import java.util.ArrayList;

public class BenchmarkTool extends Configured implements Tool {

  private LocalDirAllocator dir;
  private long startTime;

  private int BUFFER_SIZE;

  private int times;

  Configuration conf = getConf();
  long SIZE = conf.getLong("dfsthroughput.file.size",
      12L * 1024 * 1024 * 1024);
  private void resetMeasurements() { startTime = Time.now(); }

  private void printMeasurements() {
    System.out.println(" time: " +
        (Time.now() - startTime) / 1000 + " seconds");
  }

  private Path writeFile(FileSystem fs,
                                String name,
                                Configuration conf,
                                long total
                                ) throws IOException {
    System.out.println("BenchmarkTool writeFile()......");
    Path f = dir.getLocalPathForWrite(name, total, conf);
    System.out.print("Writing " + name);
    resetMeasurements();
    OutputStream out = fs.create(f);
    byte[] data = new byte[BUFFER_SIZE];
    for (long size = 0; size < total; size += BUFFER_SIZE) {
      out.write(data);
    }
    out.close();
    printMeasurements();
    return f;
  }

  private void readFile(FileSystem fs,
                               Path f,
                               String name,
                               Configuration conf
                               ) throws IOException {
    System.out.println("BenchmarkTool readFile()......");
    System.out.print("Reading " + name);
    resetMeasurements();
    InputStream in = fs.open(f);
    byte[] data = new byte[BUFFER_SIZE];
    long val = 0;
    while (val >=0) {
      val = in.read(data);
    }
    in.close();
    printMeasurements();
  }

  private void writeAndReadFile(FileSystem fs,
                                       String name,
                                       Configuration conf,
                                       long size
                                       ) throws IOException {
    System.out.println("BenchmarkTool writeAndReadFile()......");
    Path f = null;
    try {
      f = writeFile(fs, name, conf, size);
      readFile(fs, f, name, conf);
    } finally {
      try {
        if (f != null) {
          fs.delete(f, true);
        }
      } catch (IOException ie) {
        // IGNORE
      }
    }
  }

  private Path writeLocalFile(String name, Configuration conf,
                              long total) throws IOException {
    System.out.println("BenchmarkTool writeLocalFile()......");
    Path path = dir.getLocalPathForWrite(name, total, conf);
    System.out.print("Writing " + name);
    resetMeasurements();
    OutputStream out = new FileOutputStream(new File(path.toString()));
    byte[] data = new byte[BUFFER_SIZE];
    for(long size=0; size < total; size += BUFFER_SIZE) {
      out.write(data);
    }
    out.close();
    printMeasurements();
    return path;
  }

  private void writeAndReadLocalFile(String name,
                                     Configuration conf,
                                     long size
  ) throws IOException {
    System.out.println("BenchmarkTool writeAndLocalFile()......");
    Path f = null;
    try {
      f = writeLocalFile(name, conf, size);
      readLocalFile(f, name, conf);
    } finally {
      if (f != null) {
        new File(f.toString()).delete();
      }
    }
  }

  class WriteAndReadLocalFile implements Runnable {
    @Override
    public void run() {
        try {
            writeAndReadLocalFile("local", conf, SIZE);
        } catch (IOException e) {
          e.printStackTrace();
        }
    }
  }

  class WriteAndReadFile implements Runnable {
    @Override
    public void run(){
      try {
          ChecksumFileSystem checkedLocal = FileSystem.getLocal(conf);
          FileSystem rawLocal = checkedLocal.getRawFileSystem();
          writeAndReadFile(rawLocal, "raw", conf, SIZE);
          writeAndReadFile(checkedLocal, "checked", conf, SIZE);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void readLocalFile(Path path,
                             String name,
                             Configuration conf) throws IOException {
    System.out.println("BenchmarkTool readLocalFile()......");
    System.out.print("Reading " + name);
    resetMeasurements();
    InputStream in = new FileInputStream(new File(path.toString()));
    byte[] data = new byte[BUFFER_SIZE];
    long size = 0;
    while (size >= 0) {
      size = in.read(data);
    }
    in.close();
    printMeasurements();
  }

  private static void printUsage() {
    ToolRunner.printGenericCommandUsage(System.err);
    System.err.println("Only one argument here.");
    System.err.println("Config properties:\n" +
        "  dfsthroughput.file.size:\tsize of each write/read (12GB)\n" +
        "  dfsthroughput.buffer.size:\tbuffer size for write/read (4k)\n");
  }

  @Override
  public int run(String[] args) throws Exception {
    Log hadoopLog = LogFactory.getLog("org");
    if (hadoopLog instanceof Log4JLogger) {
      ((Log4JLogger) hadoopLog).getLogger().setLevel(Level.WARN);
    }
    int reps = 1;
    if (args.length == 1) {
      try {
        reps = Integer.parseInt(args[0]);
        times = reps;
      }catch (NumberFormatException e) {
        printUsage();
        return -1;
      }
    } else if (args.length > 1) {
      printUsage();
      return -1;
    }
    BUFFER_SIZE = conf.getInt("dfsthroughput.buffer.size", 4 * 1024);

    // A shared directory for temporary files.
    String localDir = conf.get("mapred.temp.dir");
    System.out.println("localDir = " + localDir);

    if (localDir == null) {
      localDir = conf.get("hadoop.tmp.dir");
      System.out.println("localDir = " + localDir);
      conf.set("mapred.temp.dir", localDir);
    }

    dir = new LocalDirAllocator("mapred.temp.dir");
    System.setProperty("test.build.data", localDir);
    System.out.println("Local = " + localDir);


    ChecksumFileSystem checkedLocal = FileSystem.getLocal(conf);
    FileSystem rawLocal = checkedLocal.getRawFileSystem();

    WriteAndReadLocalFile wdl = new WriteAndReadLocalFile();
    for (int i = 0;i<5;i++){
      Thread threadwdl = new Thread(wdl);
      threadwdl.start();
      WriteAndReadFile wd = new WriteAndReadFile();
      Thread threadwd = new Thread(wd);
      threadwd.start();
    }

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .racks(new String[]{"/foo"}).build();
      ArrayList<DataNode> listNodes = cluster.getDataNodes();
      System.out.println("listNodes's size is = " + listNodes.size()
          + "Datanode ID is = " + listNodes.get(0).getDatanodeId());
      cluster.waitActive();
      FileSystem dfs = cluster.getFileSystem();
      for(int i=0; i < reps; ++i) {
        writeAndReadFile(dfs, "dfs", conf, SIZE);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        // clean up minidfs junk
        rawLocal.delete(new Path(localDir, "dfs"), true);
      }
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new HdfsConfiguration(),
        new BenchmarkTool(), args);
    System.exit(res);
  }

}
