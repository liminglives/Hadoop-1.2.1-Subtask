/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;


public class TestMultithreadedMapRunner extends HadoopTestCase {

  public TestMultithreadedMapRunner() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  //public void testOKRun() throws Exception {
  //  run(false, false);
  //}

  //public void testIOExRun() throws Exception {
  //  run(true, false);
  //}
  //public void testRuntimeExRun() throws Exception {
  //  run(false, true);
  //}
  
  public void testWordCount() throws Exception{
	  run(true, false);
  }
  
  private void run(boolean ioEx, boolean rtEx) throws Exception {
    Path inDir = new Path("testing/mt/input");
    Path outDir = new Path("testing/mt/output");

    // Hack for local FS that does not have the concept of a 'mounting point'
    if (/*isLocalFS()*/false) {
      String localPathRoot = System.getProperty("test.build.data", "/tmp")
              .replace(' ', '+');
      inDir = new Path(localPathRoot, inDir);
      outDir = new Path(localPathRoot, outDir);
    }
    System.out.println("inDir:"+inDir);

    JobConf conf = createJobConf();
    //Configuration conf = new Configuration();
    
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    

    //Job job = new Job(conf, "WordCount");
    
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes("a b ad\nb\n\nc\nd\ne\nb");
      file.close();
    }

    conf.setJobName("mt");
    conf.setInputFormat(TextInputFormat.class);

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    //conf.setMapOutputKeyClass(LongWritable.class);
    //conf.setMapOutputValueClass(Text.class);

    //conf.setOutputFormat(TextOutputFormat.class);
    //conf.setOutputKeyClass(LongWritable.class);
    //conf.setOutputValueClass(Text.class);

    conf.setMapperClass(MapClass.class);
    conf.setCombinerClass(ReduceClass.class); // for wordcount
    conf.setReducerClass(ReduceClass.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);

    //conf.setMapRunnerClass(MultithreadedMapRunner.class);
    
    //conf.setInt("mapred.map.multithreadedrunner.threads", 2);

    //if (ioEx) {
    //  conf.setBoolean("multithreaded.ioException", true);
    //}
    //if (rtEx) {
    //  conf.setBoolean("multithreaded.runtimeException", true);
   // }
    JobClient jc = new JobClient(conf);
    RunningJob job =jc.submitJob(conf);
    while (!job.isComplete()) {
      Thread.sleep(100);
    }
    String result;
    result = readOutput(outDir, conf);
    System.out.println("result:\n" + result);

    //if (job.isSuccessful()) {
    //  assertFalse(ioEx || rtEx);
    //}
    //else {
    //  assertTrue(ioEx || rtEx);
    //}

  }
  
  public static String readOutput(Path outDir, 
          JobConf conf) throws IOException {
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

  public static class IDMap implements Mapper<LongWritable, Text,
                                              LongWritable, Text> {
    private boolean ioEx = false;
    private boolean rtEx = false;

    public void configure(JobConf job) {
      ioEx = job.getBoolean("multithreaded.ioException", false);
      rtEx = job.getBoolean("multithreaded.runtimeException", false);
    }

    public void map(LongWritable key, Text value,
                    OutputCollector<LongWritable, Text> output,
                    Reporter reporter)
            throws IOException {
      if (ioEx) {
        throw new IOException();
      }
      if (rtEx) {
        throw new RuntimeException();
      }
      output.collect(key, value);
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }


    public void close() throws IOException {
    }
  }

  public static class IDReduce implements Reducer<LongWritable, Text,
                                                  LongWritable, Text> {

    public void configure(JobConf job) {
    }

    public void reduce(LongWritable key, Iterator<Text> values,
                       OutputCollector<LongWritable, Text> output,
                       Reporter reporter)
            throws IOException {
      while (values.hasNext()) {
        output.collect(key, values.next());
      }
    }

    public void close() throws IOException {
    }
  }
  
  public static class MapClass extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
  
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  
  public void map(LongWritable key, Text value, 
                  OutputCollector<Text, IntWritable> output, 
                  Reporter reporter) throws IOException {
    String line = value.toString();
    StringTokenizer itr = new StringTokenizer(line);
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      output.collect(word, one);
    }
  }
}

  public static class ReduceClass extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable> {
  
  public void reduce(Text key, Iterator<IntWritable> values,
                     OutputCollector<Text, IntWritable> output, 
                     Reporter reporter) throws IOException {
    int sum = 0;
    while (values.hasNext()) {
      sum += values.next().get();
    }
    output.collect(key, new IntWritable(sum));
  }
}
  
}

