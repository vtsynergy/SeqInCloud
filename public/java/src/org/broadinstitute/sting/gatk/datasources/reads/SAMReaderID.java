/*
 * Copyright (c) 2010, The Broad Institute
 * Copyright (c) 2012-2013 by Virginia Polytechnic Institute and State
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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.broadinstitute.sting.gatk.datasources.reads;

import org.broadinstitute.sting.commandline.Tags;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

//import java.io.File;

import org.broadinstitute.sting.utils.exceptions.ReviewedStingException;

/**
 * Uniquely identifies a SAM file reader.
 *
 * @author mhanna
 * @version 0.1
 */
public class SAMReaderID implements Comparable {
    /**
     * The SAM file at the heart of this reader.  SAMReaderID
     * currently supports only file-based readers.
     */
    protected final File samFile;

    /* Support for HDFS */

    protected String samFileName;
    protected Path samFilePath;
    protected FileSystem samFs;
    protected FSDataInputStream samIStream;

    /**
     * A list of tags associated with this BAM file.
     */
    protected final Tags tags;

    /**
     * Creates an identifier for a SAM file based on read.
     * @param samFile The source file for SAM data.
     * @param tags tags to use when creating a reader ID.
     */
    public SAMReaderID(File samFile, String fname, Tags tags) {
        this.samFile = samFile;
	this.samFilePath = null;
        this.tags = tags;
	this.samFs = null;
	this.samIStream = null;
	this.samFileName = fname;
    }

    /**
     * Creates an identifier for a SAM file based on read.
     * @param samFileName The source filename for SAM data.
     * @param tags tags to use when creating a reader ID.
     */
    public SAMReaderID(String samFileName, Tags tags, boolean hdfs) {
	this.tags = tags;
	this.samFileName = samFileName;
	this.samFilePath = null;
	this.samFs = null;
        this.samIStream = null;
	if (hdfs == false) {
		this.samFile = new File(samFileName);
	} else {
		try {
			this.samFilePath = new Path(samFileName);
			this.samFs = samFilePath.getFileSystem(new Configuration());
			this.samIStream = samFs.open(samFilePath);
			this.samFile = null;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new ReviewedStingException("SAMReaderID constructor failed", e);
		}
	}
    }

    /**
     * Gets the absolute pathname of this SAM file
     * @return  The absolute pathname of this reader's SAM file,
     *          or null if this reader has no associated SAM file
     */
    public String getSamFilePath() {
        if ( samFile != null ) {
            return samFile.getAbsolutePath();
        } 
	else if (samFilePath != null) {
	    return samFilePath.toString();
	}
            return null;
        }

    /**
     * Gets the tags associated with the given BAM file.
     * @return A collection of the tags associated with this file.
     */
    public Tags getTags() {
        return tags;
    }

    /**
     * Compare two IDs to see whether they're equal.
     * @param other The other identifier.
     * @return True iff the two readers point to the same file.
     */
    @Override
    public boolean equals(Object other) {
        if(other == null) return false;
        if(!(other instanceof SAMReaderID)) return false;

        SAMReaderID otherID = (SAMReaderID)other;
	if (this.samFile != null)
        return this.samFile.equals(otherID.samFile);
	else if (this.samFilePath != null)
		return this.samFilePath.equals(otherID.samFilePath);
	return false;
    }

    /**
     * Generate a hash code for this object.
     * @return A hash code, based solely on the file name at this point.
     */
    @Override
    public int hashCode() {
	if (samFile != null)
        return samFile.hashCode();
	else 
		return samFilePath.hashCode();
    }

    /**
     * Best string representation for a SAM file reader is the path of the source file.
     */
    @Override
    public String toString() {
        return getSamFilePath();
    }

    @Override
    public int compareTo(Object other) {
	if (samFile != null)
        return this.samFile.getAbsolutePath().compareTo(((SAMReaderID)other).samFile.getAbsolutePath());
	else
        	return this.samFilePath.toString().compareTo(((SAMReaderID)other).samFilePath.toString());
    }
}
