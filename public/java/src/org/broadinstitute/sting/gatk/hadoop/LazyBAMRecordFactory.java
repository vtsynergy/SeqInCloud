// Copyright (c) 2011 Aalto University
// Copyright (c) 2012-2013 by Virginia Polytechnic Institute and State
// University
// All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

// File created: 2011-11-15 11:58:23

package org.broadinstitute.sting.gatk.hadoop;

import net.sf.samtools.BAMRecord;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordFactory;

public class LazyBAMRecordFactory implements SAMRecordFactory {
	@Override
	public SAMRecord createSAMRecord(SAMFileHeader hdr) {
		throw new UnsupportedOperationException(
				"LazyBAMRecordFactory can only create BAM records");
	}

	@Override
	public BAMRecord createBAMRecord(SAMFileHeader hdr,
			int referenceSequenceIndex, int alignmentStart,
			short readNameLength, short mappingQuality, int indexingBin,
			int cigarLen, int flags, int readLen,
			int mateReferenceSequenceIndex, int mateAlignmentStart,
			int insertSize, byte[] variableLengthBlock) {
		return new LazyBAMRecord(hdr, referenceSequenceIndex, alignmentStart,
				readNameLength, mappingQuality, indexingBin, cigarLen, flags,
				readLen, mateReferenceSequenceIndex, mateAlignmentStart,
				insertSize, variableLengthBlock);
	}
}

class LazyBAMRecord extends BAMRecord {
	public LazyBAMRecord(SAMFileHeader hdr, int referenceID, int coordinate,
			short readNameLength, short mappingQuality, int indexingBin,
			int cigarLen, int flags, int readLen, int mateReferenceID,
			int mateCoordinate, int insertSize, byte[] restOfData) {
		super(hdr, referenceID, coordinate, readNameLength, mappingQuality,
				indexingBin, cigarLen, flags, readLen, mateReferenceID,
				mateCoordinate, insertSize, restOfData);
	}

	@Override
	public void setReferenceIndex(final int referenceIndex) {
		mReferenceIndex = referenceIndex;
	}

	@Override
	public void setMateReferenceIndex(final int referenceIndex) {
		mMateReferenceIndex = referenceIndex;
	}

	@Override
	protected void eagerDecode() {
		getReferenceName();
		getMateReferenceName();
		super.eagerDecode();
	}
}
