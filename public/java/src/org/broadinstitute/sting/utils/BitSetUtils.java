package org.broadinstitute.sting.utils;

import org.broadinstitute.sting.utils.exceptions.ReviewedStingException;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.BitSet;

/**
 * Utilities for bitset conversion
 *
 * @author Mauricio Carneiro
 * @since 3/5/12
 */
public class BitSetUtils {

    static final private int MAX_DNA_CONTEXT = 31;                              // the maximum context size (number of bases) permitted in the "long bitset" implementation of the DNA <=> BitSet conversion.
    static final private byte NBITS_LONG_REPRESENTATION = 64;                   // the number of bits used in the long version to represent the bit set (necessary for the two's complement representation of negative numbers)
    static final private byte NBITS_SHORT_REPRESENTATION = 16;                  // the number of bits used in the short version to represent the bit set (necessary for the two's complement representation of negative numbers)
    static final long[] combinationsPerLength = new long[MAX_DNA_CONTEXT + 1];  // keeps the memoized table with the number of combinations for each given DNA context length

    /**
     * Creates an long out of a bitset
     *
     * @param bitSet the bitset
     * @return a long from the bitset representation
     */
    public static long longFrom(final BitSet bitSet) {
        return longFrom(bitSet, NBITS_LONG_REPRESENTATION);
    }

    /**
     * Creates a short integer from a bitset
     *
     * @param bitSet the bitset
     * @return a short from the bitset representation
     */
    public static short shortFrom(final BitSet bitSet) {
        return (short) longFrom(bitSet, NBITS_SHORT_REPRESENTATION);
    }

    /**
     * Cretes an integer with any number of bits (up to 64 -- long precision) from a bitset
     *
     * @param bitSet the bitset
     * @param nBits  the number of bits to be used for this representation
     * @return an integer with nBits from the bitset representation
     */
    public static long longFrom(final BitSet bitSet, final int nBits) {
        long number = 0;
        for (int bitIndex = bitSet.nextSetBit(0); bitIndex >= 0 && bitIndex <= nBits; bitIndex = bitSet.nextSetBit(bitIndex + 1))
            number |= 1L << bitIndex;

        return number;
    }

    /**
     * Creates a BitSet representation of a given long
     *
     * @param number the number to turn into a bitset
     * @return a bitset representation of the long
     */
    public static BitSet bitSetFrom(long number) {
        return bitSetFrom(number, NBITS_LONG_REPRESENTATION);
    }

    /**
     * Creates a BitSet representation of a given short
     *
     * @param number the number to turn into a bitset
     * @return a bitset representation of the short
     */
    public static BitSet bitSetFrom(short number) {
        return bitSetFrom(number, NBITS_SHORT_REPRESENTATION);
    }

    /**
     * Creates a BitSet representation of an arbitrary integer (number of bits capped at 64 -- long precision)
     *
     * @param number the number to turn into a bitset
     * @param nBits  the number of bits to use as precision for this conversion
     * @return a bitset representation of the integer
     */
    public static BitSet bitSetFrom(long number, int nBits) {
        BitSet bitSet = new BitSet();
        boolean isNegative = number < 0;
        int bitIndex = 0;
        while (number != 0) {
            if (number % 2 != 0)
                bitSet.set(bitIndex);
            bitIndex++;
            number /= 2;
        }
        if (isNegative) {
            boolean foundFirstSetBit = false;
            for (int i = bitSet.nextSetBit(0); i < nBits && i >= 0; i++) {
                boolean bit = bitSet.get(i);
                if (!foundFirstSetBit && bit)
                    foundFirstSetBit = true;    // maintain all bits until the first 1 is found (inclusive)
                else if (foundFirstSetBit)
                    bitSet.flip(i);             // flip every other bit up to NBITS_REPRESENTATION
            }
        }
        return bitSet;
    }

    /**
     * Converts a BitSet into the dna string representation.
     *
     * Warning: This conversion is limited to long precision, therefore the dna sequence cannot
     * be longer than 31 bases. To increase this limit, use BigNumbers instead of long and create
     * a bitSetFrom(BigNumber) method.
     *
     * We calculate the length of the resulting DNA sequence by looking at the sum(4^i) that exceeds the
     * base_10 representation of the sequence. This is important for us to know how to bring the number
     * to a quasi-canonical base_4 representation, and to fill in leading A's (since A's are represented
     * as 0's and leading 0's are omitted).
     *
     * quasi-canonical because A is represented by a 0, therefore,
     * instead of : 0, 1, 2, 3, 10, 11, 12, ...
     * we have    : 0, 1, 2, 3, 00, 01, 02, ...
     *
     * but we can correctly decode it because we know the final length.
     *
     * @param bitSet the bitset representation of the dna sequence
     * @return the dna sequence represented by the bitset
     */
    public static String dnaFrom(final BitSet bitSet) {
        long number = longFrom(bitSet);         // the base_10 representation of the bit set
        if (number < 0)
            throw new ReviewedStingException("dna conversion cannot handle negative numbers. Possible overflow?");

        int length = contextLengthFor(number);  // the length of the context (the number of combinations is memoized, so costs zero to separate this into two method calls)
        number -= combinationsFor(length - 1);  // subtract the the number of combinations of the preceding context from the number to get to the quasi-canonical representation

        String dna = "";
        while (number > 0) {                    // perform a simple base_10 to base_4 conversion (quasi-canonical)
            byte base = (byte) (number % 4);
            switch (base) {
                case 0:
                    dna = "A" + dna;
                    break;
                case 1:
                    dna = "C" + dna;
                    break;
                case 2:
                    dna = "G" + dna;
                    break;
                case 3:
                    dna = "T" + dna;
                    break;
            }
            number /= 4;
        }
        for (int j = dna.length(); j < length; j++)
            dna = "A" + dna;                    // add leading A's as necessary (due to the "quasi" canonical status, see description above)

        return dna;
    }

    /**
     * Creates a BitSet representation of a given dna string.
     *
     * Warning: This conversion is limited to long precision, therefore the dna sequence cannot
     * be longer than 31 bases. To increase this limit, use BigNumbers instead of long and create
     * a bitSetFrom(BigNumber) method.
     *
     * The bit representation of a dna string is the simple:
     * 0 A      4 AA     8 CA
     * 1 C      5 AC     ...
     * 2 G      6 AG     1343 TTGGT
     * 3 T      7 AT     1364 TTTTT
     *
     * To convert from dna to number, we convert the dna string to base10 and add all combinations that
     * preceded the string (with smaller lengths).
     *
     * @param dna the dna sequence
     * @return the bitset representing the dna sequence
     */
    public static BitSet bitSetFrom(String dna) {
        if (dna.length() > MAX_DNA_CONTEXT)
            throw new ReviewedStingException(String.format("DNA Length cannot be bigger than %d. dna: %s (%d)", MAX_DNA_CONTEXT, dna, dna.length()));

        long baseTen = 0;                                       // the number in base_10 that we are going to use to generate the bit set
        long preContext = combinationsFor(dna.length() - 1);    // the sum of all combinations that preceded the length of the dna string
        for (int i = 0; i < dna.length(); i++) {
            baseTen *= 4;
            switch (dna.charAt(i)) {
                case 'A':
                    baseTen += 0;
                    break;
                case 'C':
                    baseTen += 1;
                    break;
                case 'G':
                    baseTen += 2;
                    break;
                case 'T':
                    baseTen += 3;
                    break;
            }
        }
        return bitSetFrom(baseTen + preContext);                // the number representing this DNA string is the base_10 representation plus all combinations that preceded this string length.
    }

    /**
     * Calculates the number of bits necessary to represent a given number of elements
     *
     * @param numberOfElements the number of elements to represent (must be positive)
     * @return the number of bits necessary to represent this many elements
     */
    public static int numberOfBitsToRepresent(long numberOfElements) {
        if (numberOfElements < 0)
            throw new ReviewedStingException("Number of elements must be positive: " + numberOfElements);

        if (numberOfElements == 1L)
            return 1;   // special case

        int n = 0;
        numberOfElements--;
        while (numberOfElements > 0) {
            numberOfElements = numberOfElements >> 1;
            n++;
        }
        return n;
    }

    /**
     * Calculates the length of the DNA context for a given base 10 number
     *
     * It is important to know the length given the base 10 number to calculate the number of combinations
     * and to disambiguate the "quasi-canonical" state.
     *
     * This method also calculates the number of combinations as a by-product, but since it memoizes the
     * results, a subsequent call to combinationsFor(length) is O(1).
     *
     * @param number the base 10 representation of the bitset
     * @return the length of the DNA context represented by this number
     */
    private static int contextLengthFor(long number) {
        int length = 1;                              // the calculated length of the DNA sequence given the base_10 representation of its BitSet.
        long combinations = combinationsFor(length); // the next context (we advance it so we know which one was preceding it).
        while (combinations <= number) {             // find the length of the dna string (length)
            length++;
            combinations = combinationsFor(length);  // calculate the next context
        }
        return length;
    }

    /**
     * The sum of all combinations of a context of a given length from length = 0 to length.
     *
     * Memoized implementation of sum(4^i) , where i=[0,length]
     *
     * @param length the length of the DNA context
     * @return the sum of all combinations leading up to this context length.
     */
    private static long combinationsFor(int length) {
        if (length > MAX_DNA_CONTEXT)
            throw new ReviewedStingException(String.format("Context cannot be longer than %d bases but requested %d.", MAX_DNA_CONTEXT, length));

        // only calculate the number of combinations if the table hasn't already cached the value
        if (length > 0 && combinationsPerLength[length] == 0) {
            long combinations = 0L;
            for (int i = 1; i <= length; i++)
                combinations += (1L << 2 * i);        // add all combinations with 4^i ( 4^i is the same as 2^(2*i) )
            combinationsPerLength[length] = combinations;
        }
        return combinationsPerLength[length];
    }


    public static byte[] sizeOf(Object obj) throws java.io.IOException
    {
        ByteArrayOutputStream byteObject = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteObject);
        objectOutputStream.writeObject(obj);
        objectOutputStream.flush();
        objectOutputStream.close();
        byteObject.close();

        return byteObject.toByteArray();
    }
}
