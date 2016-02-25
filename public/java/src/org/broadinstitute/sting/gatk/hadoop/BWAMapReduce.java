/* Copyright (c) 2012-2013 by Virginia Polytechnic Institute and State
 * University
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT REPRESENTATIONS 
 * OR WARRANTIES OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT 
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS 
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT, OR THE 
 * ABSENCE OF LATENT OR OTHER DEFECTS, WHETHER OR NOT 
 * DISCOVERABLE. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
 * HOLDERS BE LIABLE FOR ANY CLAIM, INCIDENTAL OR CONSEQUENTIAL 
 * DAMAGES OF ANY KIND, OR OTHER LIABILITY, WHETHER IN AN ACTION 
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
 * IN THE SOFTWARE.
 */

package org.broadinstitute.sting.gatk.hadoop;

import java.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMFileReader.ValidationStringency;

public class BWAMapReduce extends Configured implements Tool {

	public static class BWAPartitioner extends Partitioner<LongWritable, Text>
			implements Configurable {
		Configuration conf;
                int nReducers;

		public BWAPartitioner() {
			conf = null;
		}

		@Override
		public void setConf(Configuration c) {
                        try {
			    conf = c;
			    if (conf.getInt("mapred.reduce.tasks", 0) == 0) {
                                ClusterStatus status = (new JobClient(new JobConf(conf))).
                                                                            getClusterStatus();
                                nReducers = status.getTaskTrackers();
                            } else {
                                nReducers = conf.getInt("mapred.reduce.tasks", 0);
                            }
                        } catch (IOException e) {
                            System.out.println("Exception thrown: " + e.getMessage());
                            System.exit(-1);
                        }
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		@Override
		public int getPartition(LongWritable key, Text value, int numPartitions) {
                        if (numPartitions == 1) {
                            return 0;
                        }
                        else {
			    int partition = (int) ((key.get() &
			                                   0x3FFFFFFFFFFFFFFFL)) - 1;
			    return (partition % nReducers);
                        }
		}
	}

	public static class BWAReducer extends
			Reducer<LongWritable, Text, NullWritable, NullWritable> {

		private Text outputValue = new Text();
		private int partition;
		private String srcPath1 = null, srcPath2 = null, dPath1 = null,
				dPath2 = null;
		private FileWriter fstream1 = null, fstream2 = null;
		private BufferedWriter out1 = null, out2 = null;
		private Configuration conf;
		private JobConf jconf;
		private Path outPath;
		String parentPath;

		public BWAReducer() {
		}

		@Override
		public void setup(Context context) throws IOException {
			conf = context.getConfiguration();
			jconf = new JobConf(conf);
			partition = conf.getInt("mapred.task.partition", 0);

			File Curdir = new File(".");
			parentPath = Curdir.getAbsolutePath();
			StringBuilder b = new StringBuilder(parentPath);
			b.replace(parentPath.lastIndexOf(File.separator), parentPath
					.lastIndexOf(File.separator) + 2, File.separator);
			parentPath = b.toString();

			srcPath1 = (new Integer(partition * 10 + 1)).toString() + "_"
					+ context.getTaskAttemptID().getTaskID().toString();
			Curdir = new File(".");
			dPath1 = parentPath.concat(srcPath1.concat(".fastq"));
			fstream1 = new FileWriter(dPath1, true);
			out1 = new BufferedWriter(fstream1);
			if (conf.getBoolean("gatk.hadoop.pairedend", false) == true) {
				srcPath2 = (new Integer(partition * 10 + 2)).toString() + "_"
						+ context.getTaskAttemptID().getTaskID().toString();
				dPath2 = (jconf.getJobLocalDir()).concat(Path.SEPARATOR
						+ srcPath2.concat(".fastq"));
				fstream2 = new FileWriter(dPath2, true);
				out2 = new BufferedWriter(fstream2);
			}

			outPath = FileOutputFormat.getOutputPath(context);
		}

		@Override
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException {
			for (Text record : values) {
				outputValue.clear();
				outputValue.append(record.getBytes(), 0, record.getLength());

			        if (conf.getBoolean("gatk.hadoop.pairedend", false) == true) {
					if ((key.get() & 0x4000000000000000L) == 0x4000000000000000L) {
						out1.append(outputValue.toString());
						out1.newLine();
					} else if ((key.get() & 0x8000000000000000L) == 0x8000000000000000L) {
						out2.append(outputValue.toString());
						out2.newLine();
					}
				} else {
					out1.append(outputValue.toString());
					out1.newLine();
				}
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
                        int nthreads;
                        boolean is_azure = conf.getBoolean("gatk.hadoop.isazure", true);
			out1.close();
			String saiPath1 = parentPath.concat(srcPath1.concat(".sai"));
			String samPath = parentPath.concat(srcPath1.concat(".sam"));

			ClusterStatus status = (new JobClient(new JobConf(context
					.getConfiguration()))).getClusterStatus();

                        if (conf.getInt("gatk.hadoop.nthreads", 0) == 0) {
			    nthreads = Math.round(status.getMaxMapTasks()
					                    / status.getTaskTrackers());
			    if (nthreads < 2)
				nthreads = 1;
                        } else {
                            nthreads = conf.getInt("gatk.hadoop.nthreads", 0);
                        }

                        String ext = "", sext;
                        if (is_azure) {
                                ext = ".exe";
                                sext = ".bat";
                        } else {
                                sext = ".sh";
                        }

			File bwaFile = new File("bwa" + ext);

			bwaFile.setExecutable(true);
			File targetFile = new File(bwaFile.getAbsolutePath());
			targetFile.setExecutable(true);

			String alnCmd1 = targetFile.toString() + " aln" + " -f " + saiPath1 + " -t "
					+ String.valueOf(nthreads) + " ref.fa " + dPath1;

                        final File f = File.createTempFile(srcPath1, sext, new File(
					                                        parentPath));

			f.setExecutable(true);
			FileWriter fstr = new FileWriter(f.toString(), true);
			BufferedWriter bw = new BufferedWriter(fstr);

			try {
			        if (conf.getBoolean("gatk.hadoop.pairedend", false) == true) {
					out2.close();
                                        String saiPath2 = parentPath.concat(srcPath2.concat(".sai"));
					String alnCmd2 = targetFile.toString() + " aln " + "-f "
							            + saiPath2 + " -t " + String.valueOf(nthreads)
							            + " ref.fa " + dPath2;
					String samCmd = targetFile.toString() + " sampe "
							            + "-r \"@RG\\tID:bwa\\tSM:dummy\\tCN:VT\" " + "-f "
							            + samPath;
                                        if (is_azure) {
                                            samCmd = samCmd + " -t " + String.valueOf(nthreads);
                                        }
                                        samCmd = samCmd +
							 " ref.fa " + saiPath1 + " " + saiPath2 + " " +
							 dPath1 + " " + dPath2;

                                        if (!is_azure) {
                                            bw.append("#!/bin/sh");
					    bw.newLine();
                                        }
					bw.append(alnCmd1);
					bw.newLine();
					bw.append(alnCmd2);
					bw.newLine();
					bw.append(samCmd);
					bw.close();
				} else {
					String samCmd = targetFile.toString() + " samse"
							            + " -r \"@RG\\tID:bwa\\tSM:dummy\\tCN:VT\" "
							            + "-f " + samPath + " ref.fa " + saiPath1 + " "
							            + dPath1;
                                        if (!is_azure) {
                                            bw.append("#!/bin/sh");
					    bw.newLine();
                                        }
					bw.append(alnCmd1);
					bw.newLine();
					bw.append(samCmd);
					bw.close();
				}

                                String[] cmds;
                                if (is_azure) {
				    cmds = new String[] { "cmd.exe", "/C",
						                        f.getAbsolutePath() };
                                } else {
                                    cmds = new String[] {f.getAbsolutePath()};
                                }
				final Process pr = new ProcessBuilder(cmds)
						                .redirectErrorStream(true).start();
				BufferedReader in = new BufferedReader(new InputStreamReader(pr
						                                    .getInputStream()));
				String line = null;
				while ((line = in.readLine()) != null) {
				}

				final int exitVal = pr.waitFor();

				File samFile = new File(samPath);
				SAMFileReader fileReader = new SAMFileReader(samFile);
				SAMFileHeader fileHeader = fileReader.getFileHeader();
				String hdr = new String(fileHeader.getTextHeader());
				String fullheader = "@HD\tVN:1.0\tSO:coordinate\n" + hdr;
				StringBuilder b = new StringBuilder(fullheader);
				b.replace(fullheader.lastIndexOf("\n"), fullheader
						                    .lastIndexOf("\n") + 1, " ");
				fileHeader.setTextHeader(b.toString());
				fileReader.close();

				fileReader = new SAMFileReader(samFile);
				fileReader.enableFileSource(true);
				fileReader.setValidationStringency(ValidationStringency.SILENT);

				int part = context.getTaskAttemptID().getTaskID().getId();

				Path outputPath = new Path(outPath.toString() + Path.SEPARATOR
						                    + String.format("%06d", part) + "-"
						                    + context.getTaskAttemptID().getTaskID().toString()
						                    + ".bam");

				BAMRecordWriterLite writer = new BAMRecordWriterLite(
						                outputPath, fileHeader, true, conf);

				for (SAMRecord rec : fileReader) {
					writer.writeAlignment(rec);
				}

				fileReader.close();
				writer.close();

			} catch (Exception e) {
				System.out
						.println("Command Execution Error: " + e.getMessage());
			}
		}
	}

	public int run(String[] args) throws Exception {
		return 0;
	}
}
