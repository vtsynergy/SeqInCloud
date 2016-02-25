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

import org.broadinstitute.sting.utils.exceptions.UserException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.Tool;
import net.sf.samtools.SAMFileReader.ValidationStringency;

import net.sf.samtools.BAMIndexer;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.picard.sam.MarkDuplicates;
import net.sf.picard.sam.FixMateInformation;

import java.lang.*;

public class IndexMapReduce extends Configured implements Tool {

	public static class IndexMapper extends
			Mapper<LongWritable, Text, NullWritable, NullWritable> {

		@Override
		public void setup(Context context) throws IOException {
		}

		public void map(LongWritable key, Text value, Context context) {

                        Configuration conf;
                        SAMFileReader fileReader;
                        BAMIndexer indexer;
                        SplittingBAMIndexer sindexer;
                        Path indexPath, input;
                        int index;
                        String identifier, outputFile;
                        JobConf job;
                        boolean markDup;

                        try {
                            conf = context.getConfiguration();
                            markDup = conf.getBoolean("gatk.hadoop.ismarkdup", false);
                            job = new JobConf(conf);
                            input = new Path(value.toString());

                            // FixMateInformation and MarkDuplicates 
                            if (markDup) { 
                                index = input.getName().indexOf("-");
                                identifier = input.getName().substring(0, index);

                                // Picard MarkDuplicates stage. Need to copy the file to Local
                                // FS as we cannot operate using streams
                                String inputFile = new String(job.getJobLocalDir()
                                                    + Path.SEPARATOR
                                                    + context.getTaskAttemptID().toString() + ".bam");
                                String metricsFile = inputFile + ".metrics";
                                outputFile = new String(job.getJobLocalDir()
                                                    + Path.SEPARATOR
                                                    + context.getTaskAttemptID().toString() + "_rmdup.bam");
                                (input.getFileSystem(conf))
                                                    .copyToLocalFile(false, input, new Path(inputFile));

                                ArrayList<String> list = new ArrayList<String>();

                                // MarkDuplicates Stage

                                list.add("INPUT=" + inputFile);
                                list.add("METRICS_FILE=" + metricsFile);
                                list.add("VALIDATION_STRINGENCY=SILENT");
                                list.add("REMOVE_DUPLICATES=true"); // good to make this optional
                                list.add("ASSUME_SORTED=true");
                                list.add("OUTPUT=" + outputFile);

                                String[] argv = new String[list.size()];
                                argv = new String[list.size()];
                                list.toArray(argv);

                                int returnValue = new MarkDuplicates().instanceMain(argv);

                                // Indexing Using HDFS

                                input = new Path(FileOutputFormat
                                                .getWorkOutputPath(context), identifier + "-rmdup.bam");
                                (input.getFileSystem(conf))
                                                .copyFromLocalFile(false, true, new Path(outputFile),
                                                                                                input);
                            }

                            // Indexing 
                            if( (markDup && conf.getBoolean("gatk.hadoop.isindex", false)) ||
                                                        (conf.getBoolean("gatk.hadoop.isindex", false)) ) {
                                FSDataInputStream instream = input.getFileSystem(
                                                                    conf).open(input);
                                fileReader = new SAMFileReader(instream);
                                fileReader.setValidationStringency(ValidationStringency.SILENT);
                                fileReader.enableFileSource(true);

                                if (markDup) {
                                    indexPath = new Path(FileOutputFormat
                                                        .getWorkOutputPath(context).toString()
                                                            + Path.SEPARATOR + "_" + input.getName() + ".bai");
                                } else {
                                    indexPath = new Path((input.getParent()).toString() + Path.SEPARATOR + "_" +
                                                                                            input.getName() + ".bai");
                                }

                                FileSystem fsi = indexPath.getFileSystem(context
                                                                    .getConfiguration());

                                if (!fsi.exists(indexPath)) {

                                    FSDataOutputStream out = fsi.create(indexPath, true);

                                    indexer = new BAMIndexer(out, fileReader.getFileHeader());

                                    for (SAMRecord rec : fileReader) {
                                        indexer.processAlignment(rec);
                                    }

                                    indexer.finish();
                                }
                                fileReader.close();
                                instream.close();
                            }

                            // SplittingBAMIndexing
                            if( (markDup && conf.getBoolean("gatk.hadoop.issindex", false)) ||
                                                        (conf.getBoolean("gatk.hadoop.issindex", false)) ) {
		                String pstr = input.toString();
		                Path sIndexPath = new Path(pstr.replace(input.getName(), "_" + input.getName()
				                                                        + ".splitting-bai"));
                                FileSystem fsi = sIndexPath.getFileSystem(context
                                                                            .getConfiguration());

                                if (!fsi.exists(sIndexPath)) {
                                    sindexer = new SplittingBAMIndexer(conf.getInt("hadoop.gatk.granularity", 100));
                                    try {
                                        sindexer.index(input, new Configuration());
                                    } catch (IOException e) {
                                        System.out.println("Indexing Failed!: " + e.getMessage());
                                        System.exit(-1);
                                    }
                                }
                            }

                            /*
                            // Indexing Using LocalFS if markDup enabled
                            if (markDup) {
                                fileReader = new SAMFileReader(new File(outputFile));
                                fileReader.setValidationStringency(ValidationStringency.SILENT);
                                fileReader.enableFileSource(true);

                                String indexFile = new String(job.getJobLocalDir()
                                                + Path.SEPARATOR
                                                + context.getTaskAttemptID().toString() + ".bam.bai");

                                indexer = new BAMIndexer(new File(indexFile), fileReader
                                                .getFileHeader());

                                for (SAMRecord rec : fileReader) {
                                        indexer.processAlignment(rec);
                                }

                                indexer.finish();
                                fileReader.close();

                                sindexer = new SplittingBAMIndexer(conf.getInt("hadoop.gatk.granularity", 100));
                                try {
                                        sindexer.index(new File(outputFile));
                                } catch (IOException e) {
                                        System.out.println("Indexing Failed!: " + e.getMessage());
                                        System.exit(-1);
                                }

                                // Copy everything back to HDFS

                                Path rmdupPath = new Path(FileOutputFormat
                                                .getWorkOutputPath(context), identifier + "-rmdup.bam");
                                FileSystem fs = rmdupPath.getFileSystem(context
                                                                            .getConfiguration());
                                fs.copyFromLocalFile(false, true, new Path(outputFile),
                                                                                rmdupPath);
                                indexPath = new Path(FileOutputFormat
                                                    .getWorkOutputPath(context), identifier
                                                                                + "-rmdup.bam.bai");
                                fs.copyFromLocalFile(false, true, new Path(indexFile),
                                                                                    indexPath);
                                Path sindexPath = new Path(rmdupPath.toString()
                                                                    + ".splitting-bai");
                                fs.copyFromLocalFile(false, true, new Path(outputFile
                                                                + ".splitting-bai"), sindexPath);
                            }
                            */

                        } catch (UserException e) {
                                System.out.println(e.getMessage());
                                System.exit(-1);
                        } catch (net.sf.samtools.SAMException e) {
                                System.out.println(e.getMessage());
                                System.exit(-1);
                        } catch (Throwable t) {
                                System.out.println(t.getMessage());
                                System.exit(-1);
                        }
                    }
            }

            public int run(String[] args) throws Exception {
                    return 0;
            }
    }
