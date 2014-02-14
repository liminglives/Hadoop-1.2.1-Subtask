package org.apache.hadoop.mapreduce.lib.map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.WordCount;
import org.apache.hadoop.mapreduce.WordCount.IntSumReducer;
import org.apache.hadoop.mapreduce.WordCount.TokenizerMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;


public class TestWordCount extends HadoopTestCase {

  public TestWordCount() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }
  
  public void testWordCount() throws Exception{
	  run(true, false);
  }
  
  private void run(boolean ioEx, boolean rtEx) throws Exception {
    Path inDir = new Path("testing/mt/input");
    Path outDir = new Path("testing/mt/output");

    System.out.println("inDir:"+inDir);

    //JobConf conf = createJobConf();
    Configuration conf = new Configuration();
    
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    
    conf.setBoolean("mapreduce.subtask.on", false);
    conf.setBoolean("mapreduce.subtask.output.on", false);
    conf.setBoolean("mapreduce.sub.reducetask.on", false);
    
    Job job = new Job(conf, "WordCount");
    
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      for (int i=0; i<1000; ++i)
        file.writeBytes("a b ad\nb\n\nc\nd\ne\nba b ad\nb\n\nc\nd\ne\nba b ad\nb\n\nc\nd\ne\n");
      file.close();
    }
    System.out.println("inFs:"+inFs);
    
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir); 
    
    if(!job.waitForCompletion(true))
    	System.out.println("job word count failed");

    String result;
    result = readOutput(outDir, job.getConfiguration());
    System.out.println("result:\n" + result);



  }
  
  public static String readOutput(Path outDir, 
          Configuration conf) throws IOException {
    FileSystem fs = outDir.getFileSystem(conf);
    StringBuffer result = new StringBuffer();
    {

    Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
    new Utils.OutputFileUtils.OutputFilesFilter()));

    for(int i=0; i < fileList.length; ++i) {
    System.out.println("File list[" + i + "]" + ": "+ fileList[i]);
    BufferedReader file = 
    new BufferedReader(new InputStreamReader(fs.open(fileList[i])));
    String line = file.readLine();
    while (line != null) {
      result.append(line);
      result.append("\n");
      line = file.readLine();
      }
    file.close();
    }
  }
  return result.toString();
 }
  
}

