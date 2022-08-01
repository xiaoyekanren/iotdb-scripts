package model;


/**
 * @author lta
 */
public class WriteDataToBytes  {

    public void write(byte[] data, int value, int bitOffset) {
        for (int i = 0; i < 8; i ++){
            if(((value >> (7 - i)) & 1) ==1) {
                write1ToBytes(data, bitOffset + i);
            }else {
                write0ToBytes(data, bitOffset + i);
            }
        }
    }

    public void write1ToBytes(byte[] data, int bitOffset){
        int byteBoundary;
        int offsetInByte;
        byteBoundary = bitOffset >> 3;
        offsetInByte = bitOffset & 7;
	    data[byteBoundary] |= (1 << (7 - offsetInByte));
    }

    public void write0ToBytes(byte[] data, int bitOffset) {
        int byteBoundary;
        int offsetInByte;
        byteBoundary = bitOffset >> 3;
        offsetInByte = bitOffset & 7;
	    data[byteBoundary] &= (~(1 << (7 -  offsetInByte)));
    }
}
