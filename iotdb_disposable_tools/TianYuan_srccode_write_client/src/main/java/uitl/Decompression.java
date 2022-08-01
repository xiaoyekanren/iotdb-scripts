package uitl;

import ty.pub.RawDataPacket;

public class Decompression {
	public static RawDataPacket decom(RawDataPacket rawDataPacket, String imei, String signal) {
		// region 7E源码解压缩
		// try {
		if (signal.startsWith("7E") && !"888888".equals(rawDataPacket.getServerPort())) {
			int dtVer = 2;
			String editionVer = DataTranslator.subString(signal, 22, 4);
			editionVer = DataTranslator.subString(signal, 26, 4);
			if ("1061".equals(editionVer)) {
				dtVer = 1;// 透传一期
			}

			String parameterOption = DataTranslator.subString(signal, 28, 4);
			String bitOption = DataTranslator.hexToBin(parameterOption);
			// lz77标志
			boolean lz77Flag = false;
			if (dtVer == 2 && "1".equals(DataTranslator.subString(bitOption, 1, 1))) {
				lz77Flag = true;
			}

			if (lz77Flag) {
				// 消息体
				String decompress = "";
				// 消息体长度
				int length = DataTranslator.hexToDecimal(DataTranslator.subString(signal, 44, 4));
				int byte1 = length / 8;// 整字节的长度
				int byte2 = length % 8;// 最后一个字节的位数
				// 先取到byte1的长度
				String str1 = DataTranslator.subString(signal, 48, byte1 * 2);
				// 如果余数不等于0，则取下一个字节的位长度，取完以后转换成整字节
				String str2 = "";
				if (byte2 != 0) {
					str2 = DataTranslator.subString(signal, 48 + byte1 * 2, 2);
				}
				// 拼接两个长度
				String content = str1 + str2;

				decompress = DataTranslator.decompress(content, length);
				String s1 = signal.substring(0, 48);
				String s2 = signal.substring(signal.length() - 6);

				signal = s1 + decompress + s2;
			}
		}
		// } catch (Exception e) {
		// e.printStackTrace();
		// System.out.println(imei);
		// System.out.println(signal);
		// }
		rawDataPacket.setPacketData(signal);
		// endregion

		return rawDataPacket;
	}

}
