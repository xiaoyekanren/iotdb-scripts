package model;

/**
 * The class Match is the present a pair of macth data's length and offset.
 * @author lta
 */
public class Match {
    private int length;      //length of match byte data
    private int bitOffset;   //offset of the first bit data to be decompressed in compressed byte array


    public Match() {
        this(-1, -1);
    }

    public Match(int matchLength, int matchIndex) {
        super();
        this.length = matchLength;
        this.bitOffset = matchIndex;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int matchLength) {
        this.length = matchLength;
    }

    public int getBitOffset() {
        return bitOffset;
    }

    public void setBitOffset(int matchIndex) {
        this.bitOffset = matchIndex;
    }

}
