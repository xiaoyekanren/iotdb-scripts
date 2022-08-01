package uitl;

import model.Match;
import model.ReadDataFromBytes;
import model.WriteDataToBytes;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class DataTranslator {

	// region 字符串左补齐

	/**
	 * 字符串左补齐
	 *
	 * @param src
	 *            原字符串
	 * @param len
	 *            补齐后总长度
	 * @param ch
	 *            左补齐用的字符
	 **/
	public static String padLeft(String src, int len, char ch) {
		int diff = len - src.length();
		if (diff <= 0) {
			return src;
		}

		char[] charr = new char[len];
		System.arraycopy(src.toCharArray(), 0, charr, diff, src.length());
		for (int i = 0; i < diff; i++) {
			charr[i] = ch;
		}
		return new String(charr);
	}
	// endregion

	// region 十六进制转十进制

	/**
	 * 十六进制转十进制（int）
	 */
	public static int hexToDecimal(String hexString) {

		return Integer.parseInt(hexString, 16);
	}

	/**
	 * 十六进制转十进制（long）
	 */
	public static long hexToLong(String hexString) {

		return Long.parseLong(hexString, 16);
	}

	// endregion

	// region 十六进制转二进制

	/**
	 * 十六进制转二进制
	 */
	public static String hexToBin(String hex) {
		int length = hex.length();
		StringBuilder sb = new StringBuilder(length * 4);

		for (int i = 0; i < length; i++) {
			switch (hex.charAt(i)) {
				case '0':
					sb.append("0000");
					break;
				case '1':
					sb.append("0001");
					break;
				case '2':
					sb.append("0010");
					break;
				case '3':
					sb.append("0011");
					break;
				case '4':
					sb.append("0100");
					break;
				case '5':
					sb.append("0101");
					break;
				case '6':
					sb.append("0110");
					break;
				case '7':
					sb.append("0111");
					break;
				case '8':
					sb.append("1000");
					break;
				case '9':
					sb.append("1001");
					break;
				case 'A':
				case 'a':
					sb.append("1010");
					break;
				case 'B':
				case 'b':
					sb.append("1011");
					break;
				case 'C':
				case 'c':
					sb.append("1100");
					break;
				case 'D':
				case 'd':
					sb.append("1101");
					break;
				case 'E':
				case 'e':
					sb.append("1110");
					break;
				case 'F':
				case 'f':
					sb.append("1111");
					break;
				default:
					return sb.toString();
			}
		}
		return sb.toString();
	}
	// endregion

	// region 获取年月日时分秒时间类型，形参为16进制时间字符串

	/**
	 * 获取年月日时分秒时间类型，形参为16进制时间字符串，异常时间会返回0
	 *
	 * @param data
	 *            长度为12或者为14的16进制字符串
	 */

	public static long getLongDateTime(String data) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// 设置异常日期会报错，比如2019-11-31 00:00:00
		sdf.setLenient(false);

		try {
			if (data.length() != 12 && data.length() != 14) {
				return 0;
			}

			String year = "";
			int month;
			int day;
			String hours;
			String minute;
			String second;
			String time = "";

			int startPosition = 0;
			if (data.length() == 12) {
				year = "2" + padLeft(
						String.valueOf(Integer.parseInt(data.substring(startPosition, startPosition + 2), 16)), 3, '0');// 取得日期的年(一字节)
				startPosition += 2;
			} else if (data.length() == 14) {
				year = String.valueOf(Integer.parseInt(data.substring(startPosition, startPosition + 4), 16));// 取得日期的年(两字节)
				startPosition += 4;
			}

			month = (Integer.parseInt(data.substring(startPosition, startPosition + 2), 16));// 取得日期的月(一字节)
			startPosition += 2;
			day = (Integer.parseInt(data.substring(startPosition, startPosition + 2), 16));// 取得日期的天(一字节)
			startPosition += 2;

			hours = String.valueOf(Integer.parseInt(data.substring(startPosition, startPosition + 2), 16));// 取得日期的小时(一字节)
			startPosition += 2;
			minute = String.valueOf(Integer.parseInt(data.substring(startPosition, startPosition + 2), 16));// 取得日期的分钟(一字节)
			startPosition += 2;
			second = String.valueOf(Integer.parseInt(data.substring(startPosition, startPosition + 2), 16));// 取得日期的秒(一字节)
			startPosition += 2;
			time = year + "-" + month + "-" + day + " " + hours + ":" + minute + ":" + second;
			// 此处做了特殊逻辑，有可能是全是0，但是本条数据也做处理。
			if ("2000-0-0 0:0:0".equals(time) || "2255-255-255 255:255:255".equals(time)
					|| "65535-255-255 255:255:255".equals(time)) {
				return 0;
			} else {
				return sdf.parse(time).getTime();
			}
		} catch (Exception ex) {
			return 0;
		}
	}
	// endregion

	// region Net版本SubString方法

	/**
	 * Net版本SubString方法
	 *
	 * @param s
	 *            源字符串
	 * @param index
	 *            字符串开始索引
	 * @param length
	 *            截取长度
	 * @return 截取的字符串
	 */
	public static String subString(String s, int index, int length) {
		int Endposition = length + index;
		return s.substring(index, Endposition);
	}
	// endregion

	// region LZ77 Java解压缩算法
	private static final int MAX_WINDOW_SIZE = 1 << 10;
	private static final int OFFSET_CODING_LENGTH = 10;
	private static ReadDataFromBytes readDataFromBytes = new ReadDataFromBytes();
	private static WriteDataToBytes writeDataToBytes = new WriteDataToBytes();

	/**
	 * LZ77 Java字节解压缩算法
	 *
	 * @param compressedData
	 *            压缩后的字节数组
	 * @param bitsTotalLength
	 *            压缩后的字节数组中的有效bit位数
	 *
	 * @return 解压缩后的字节数组
	 */
	public byte[] decompress(byte[] compressedData, int bitsTotalLength) {
		List<Byte> decompressedData = new ArrayList<>();
		int bitOffset = 0; // the first bit to decompress
		int resultByteOffset = 0;
		int slideWindowByteOffset = 0;
		while (bitsTotalLength > bitOffset) {
			int bit = readDataFromBytes.readBit(compressedData, bitOffset);
			bitOffset++;
			if (bit == 1) {
				if (resultByteOffset >= MAX_WINDOW_SIZE)
					slideWindowByteOffset = resultByteOffset - MAX_WINDOW_SIZE;
				else
					slideWindowByteOffset = 0;
				int offset = readDataFromBytes.read(compressedData, OFFSET_CODING_LENGTH, bitOffset);
				bitOffset += 10;
				Match match = readDataFromBytes.readGolombCode(compressedData, bitOffset);
				int length = match.getLength();
				bitOffset = match.getBitOffset();
				for (int i = 0; i < length; i++) {
					int byteData = readDataFromBytes.read(decompressedData, 8,
							(slideWindowByteOffset + offset + i) * 8);
					decompressedData.add((byte) byteData);
					resultByteOffset++;
				}
			} else {
				int byteData = 0;
				for (int i = 0; i < 8; i++, bitOffset++) {
					bit = readDataFromBytes.readBit(compressedData, bitOffset);
					byteData |= (bit << i);
				}
				decompressedData.add((byte) byteData);
				resultByteOffset++;
			}
		}
		byte[] result = new byte[decompressedData.size()];
		for (int i = 0; i < decompressedData.size(); i++)
			result[i] = decompressedData.get(i);
		return result;
	}

	/**
	 * LZ77 Java解压缩算法
	 * 
	 * @param compressedData
	 *            压缩后的字节数组
	 * @param decompressedData
	 *            解 压缩后的字节数组
	 * @param bitsTotalLength
	 *            压缩后的字节数组中的有效bit位数
	 * @return 解压缩后的字节数组的有效长度
	 */
	public int decompress(byte[] compressedData, byte[] decompressedData, int bitsTotalLength) {
		int bitOffset = 0; // the first bit to decompress
		int resultByteOffset = 0;
		int slideWindowByteOffset = 0;
		while (bitsTotalLength > bitOffset) {
			int bit = readDataFromBytes.readBit(compressedData, bitOffset);
			bitOffset++;
			if (bit == 1) {
				if (resultByteOffset >= MAX_WINDOW_SIZE)
					slideWindowByteOffset = resultByteOffset - MAX_WINDOW_SIZE;
				else
					slideWindowByteOffset = 0;
				int offset = readDataFromBytes.read(compressedData, OFFSET_CODING_LENGTH, bitOffset);
				bitOffset += 10;
				Match match = readDataFromBytes.readGolombCode(compressedData, bitOffset);
				int length = match.getLength();
				bitOffset = match.getBitOffset();
				for (int i = 0; i < length; i++) {
					int byteData = readDataFromBytes.read(decompressedData, 8,
							(slideWindowByteOffset + offset + i) * 8);
					writeDataToBytes.write(decompressedData, byteData, resultByteOffset * 8);
					resultByteOffset++;
				}
			} else {
				int byteData = 0;
				for (int i = 0; i < 8; i++, bitOffset++) {
					bit = readDataFromBytes.readBit(compressedData, bitOffset);
					byteData |= (bit << i);
				}
				writeDataToBytes.write(decompressedData, byteData, resultByteOffset * 8);
				resultByteOffset++;
			}
		}
		return resultByteOffset;
	}

	/**
	 * Java Lz77解压缩算法
	 * 
	 * @param compressedDataHex
	 *            压缩的16进制字符串
	 * @param bitsTotalLength
	 *            有效长度
	 * @return 解压缩后的16进制字符串
	 */
	public static String decompress(String compressedDataHex, int bitsTotalLength) {
		int bitOffset = 0; // the first bit to decompress
		int resultByteOffset = 0;
		int slideWindowByteOffset = 0;
		byte[] compressedData = new byte[10000];
		byte[] decompressedData = new byte[10000];
		convertHexToBinary(compressedData, compressedDataHex);
		while (bitsTotalLength > bitOffset) {
			int bit = readDataFromBytes.readBit(compressedData, bitOffset);
			bitOffset++;
			if (bit == 1) {
				if (resultByteOffset >= MAX_WINDOW_SIZE)
					slideWindowByteOffset = resultByteOffset - MAX_WINDOW_SIZE;
				else
					slideWindowByteOffset = 0;
				int offset = readDataFromBytes.read(compressedData, OFFSET_CODING_LENGTH, bitOffset);
				bitOffset += 10;
				Match match = readDataFromBytes.readGolombCode(compressedData, bitOffset);
				int length = match.getLength();
				bitOffset = match.getBitOffset();
				for (int i = 0; i < length; i++) {
					int byteData = readDataFromBytes.read(decompressedData, 8,
							(slideWindowByteOffset + offset + i) * 8);
					writeDataToBytes.write(decompressedData, byteData, resultByteOffset * 8);
					resultByteOffset++;
				}
			} else {
				int byteData = 0;
				for (int i = 0; i < 8; i++, bitOffset++) {
					bit = readDataFromBytes.readBit(compressedData, bitOffset);
					byteData |= (bit << i);
				}
				writeDataToBytes.write(decompressedData, byteData, resultByteOffset * 8);
				resultByteOffset++;

			}
		}
		return convertBinaryToHex(decompressedData, resultByteOffset);
	}

	public static void convertHexToBinary(byte[] output, String hexData) {
		int bitOffset = 0;
		for (int i = 0; i < hexData.length(); i += 2) {
			char firstChar = hexData.charAt(i);
			char lastChar = hexData.charAt(i + 1);
			int first, last;
			if (firstChar > '9') {
				first = firstChar - 'A' + 10;
			} else
				first = firstChar - '0';
			if (lastChar > '9') {
				last = lastChar - 'A' + 10;
			} else
				last = lastChar - '0';
			for (int j = 0; j < 4; j++) {
				int bit = (first >> (3 - j)) & 1;
				if (bit == 1) {
					writeDataToBytes.write1ToBytes(output, bitOffset++);
				} else {
					writeDataToBytes.write0ToBytes(output, bitOffset++);
				}
			}
			for (int j = 0; j < 4; j++) {
				int bit = (last >> (3 - j)) & 1;
				if (bit == 1) {
					writeDataToBytes.write1ToBytes(output, bitOffset++);
				} else {
					writeDataToBytes.write0ToBytes(output, bitOffset++);
				}
			}
		}
	}

	/**
	 * 根据字节数组的有效长度转16进制字符串，配合decompress(byte[] compressedData, byte[]
	 * decompressedData, int bitsTotalLength)使用
	 * 
	 * @param input
	 *            字节数组
	 * @param decompressedDataLength
	 *            该数组的有效长度
	 * @return 16进制字符串
	 */
	public static String convertBinaryToHex(byte[] input, int decompressedDataLength) {
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < decompressedDataLength; i++) {
			byte temp = input[i];
			int bit = (temp >> 4) & 0x0f;
			if (bit > 9) {
				result.append((char) ('A' + bit - 10));
			} else {
				result.append((char) ('0' + bit));
			}
			bit = (char) (temp & 0x0f);
			if (bit > 9) {
				result.append((char) ('A' + bit - 10));
			} else {
				result.append((char) ('0' + bit));
			}
		}
		return result.toString();
	}
	// endregion

}
