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

package org.broadinstitute.sting.gatk.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * NLineInputFormat which splits N lines of input as one split.
 *
 * In many "pleasantly" parallel applications, each process/mapper 
 * processes the same input file (s), but with computations are 
 * controlled by different parameters.(Referred to as "parameter sweeps").
 * One way to achieve this, is to specify a set of parameters 
 * (one set per line) as input in a control file 
 * (which is the input path to the map-reduce application,
 * where as the input dataset is specified 
 * via a config variable in JobConf.).
 * 
 * The NLineInputFormat can be used in such applications, that splits 
 * the input file such that by default, one line is fed as
 * a value to one map task, and key is the offset.
 * i.e. (k,v) is (LongWritable, Text).
 * The location hints will span the whole mapred cluster.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NLineXInputFormat extends 
                                FileInputFormat<LongWritable, Text> { 
  public static final String LINES_PER_MAP = 
                        "mapred.line.input.format.linespermap";

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(
      InputSplit genericSplit, TaskAttemptContext context) 
      throws IOException {
    context.setStatus(genericSplit.toString());
    return new NLineXRecordReader();
  }

  /** 
   * Logically splits the set of input files for the job, splits N lines
   * of the input as one split.
   * 
   * @see FileInputFormat#getSplits(JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext job)
                throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    int numLinesPerSplit = getNumLinesPerSplit(job);
    int fileId = 1;
    for (FileStatus status : listStatus(job)) {
      if (((job.getConfiguration()).getBoolean("gatk.hadoop.pairedend", false)) &&
               ((job.getConfiguration()).get("gatk.hadoop.readfile2")).contains((status.getPath()).getName())) {
        fileId = 2;
      }
      splits.addAll(getSplitsForFile(status,
                        job.getConfiguration(), numLinesPerSplit, fileId));
    }
    return splits;
  }
  
  public static List<InputSplit> getSplitsForFile(FileStatus status,
                    Configuration conf, int numLinesPerSplit, long fileid) 
                        throws IOException 
  {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    Path fileName = status.getPath();
    long splitId = 1;
    if (status.isDir()) {
      throw new IOException("Not a file: " + fileName);
    }
    FileSystem  fs = fileName.getFileSystem(conf);
    LineReader lr = null;
    try {
      FSDataInputStream in  = fs.open(fileName);
      lr = new LineReader(in, conf);
      Text line = new Text();
      int numLines = 0;
      long begin = 0;
      long length = 0;
      int num = -1;
      while ((num = lr.readLine(line)) > 0) {
        numLines++;
        length += num;
        if (numLines == numLinesPerSplit) {
          // NLineInputFormat uses LineRecordReader, which always reads
          // (and consumes) at least one character out of its upper split
          // boundary. So to make sure that each mapper gets N lines, we
          // move back the upper split limits of each split 
          // by one character here.
          if (begin == 0) {
            splits.add(new XFileSplit(fileName, begin, length - 1, fileid, splitId++,
                                                                        new String[] {}));
          } else {
            splits.add(new XFileSplit(fileName, begin - 1, length, fileid, splitId++,
                                                                        new String[] {}));
          }
          begin += length;
          length = 0;
          numLines = 0;
        }
      }
      if (numLines != 0) {
        splits.add(new XFileSplit(fileName, begin, length, fileid, splitId++, 
                                                                    new String[]{}));
      }
    } finally {
      if (lr != null) {
        lr.close();
      }
    }
    return splits; 
  }
  
  /**
   * Set the number of lines per split
   * @param job the job to modify
   * @param numLines the number of lines per split
   */
  public static void setNumLinesPerSplit(Job job, int numLines) {
    job.getConfiguration().setInt(LINES_PER_MAP, numLines);
  }

  /**
   * Get the number of lines per split
   * @param job the job
   * @return the number of lines per split
   */
  public static int getNumLinesPerSplit(JobContext job) {
    return job.getConfiguration().getInt(LINES_PER_MAP, 1);
  }
}
