package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by mingleiz on 8/6/2015.
 */
public class BenchmarkThroughputEC extends Configured implements Tool {



  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = getConf();

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new HdfsConfiguration(),
        new BenchmarkThroughputEC(), args);
    System.exit(res);
  }

}
