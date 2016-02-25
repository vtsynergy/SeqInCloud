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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.util.Tool;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SAMRecordWritable;

public class ContigMapReduce extends Configured implements Tool {

	public static class ContigPartitioner extends Partitioner<LongWritable, SAMRecordWritable>
			implements Configurable {
		Configuration conf;

		public ContigPartitioner() {
			conf = null;
		}

		@Override
		public void setConf(Configuration c) {
		    conf = c;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		@Override
		public int getPartition(LongWritable key, SAMRecordWritable value, int numPartitions) {
                    int partition = (int)((key.get() >>> 32) & 0x00000000FFFFFFFFL);
		    return partition;
		}
	}

	public int run(String[] args) throws Exception {
		return 0;
	}
}
