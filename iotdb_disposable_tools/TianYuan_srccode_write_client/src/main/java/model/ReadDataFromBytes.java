package model;


import java.util.List;

/**
 * @author lta
 */
public class ReadDataFromBytes  {

    private static final int m = 3;               //Golomb code parameter

    public int read(byte[] data, int n, int bitOffset) {
        int output = 0;
        for (int i = 0; i < n; i++) {
            int val = readBit(data, bitOffset + i);
            if (val == 1) {
                output = output | (1 << i);
            }
        }
        return output;
    }

    public int read(List<Byte> data, int n, int bitOffset) {
        int output = 0;
        for (int i = 0; i < n; i++) {
            int val = readBit(data, bitOffset + i);
            if (val == 1) {
                output = output | (1 << i);
            }
        }
        return output;
    }

    public int readBit(byte[] data, int bitOffset) {
        int byteBoundary;
        int offsetInByte;
        byteBoundary = bitOffset >> 3;
        offsetInByte = bitOffset & 7;
        return (data[byteBoundary] >> offsetInByte) & 1;
    }

    public int readBit(List<Byte> data, int bitOffset) {
        int byteBoundary;
        int offsetInByte;
        byteBoundary = bitOffset >> 3;
        offsetInByte = bitOffset & 7;
        return (data.get(byteBoundary) >> offsetInByte) & 1;
    }

    public Match readGolombCode(byte[] data, int bitOffset) {
		int q, r = 0;
		int bit;
		Match match = new Match();
		for (q = 0;; q++) {
			bit = readBit(data, bitOffset);
			bitOffset++;
			if (bit == 0) {
				break;
			}
		}
		for (int i = 0; i < m; i++, bitOffset++) {
			bit = readBit(data, bitOffset);
			bit <<= i;
			r |= bit;
		}
		match.setLength(r + (q << m) + 1);
        match.setBitOffset(bitOffset);
        return match;
    }
}
