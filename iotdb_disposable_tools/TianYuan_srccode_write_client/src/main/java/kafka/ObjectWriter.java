package kafka;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import ty.pub.TransPacket;

public class ObjectWriter {

	static String writeTransPacket(TransPacket transPacket) {
		String result = "";
		Map<String, Map<Long, String>> workStatusMap = transPacket.getWorkStatusMap();
		for (Entry<String, Map<Long, String>> workStatusData : workStatusMap.entrySet()) {
			String workStatusName = workStatusData.getKey();
			result += workStatusName + "\n ";
			for (Entry<Long, String> timeOffsetValuePair : workStatusData.getValue().entrySet()) {
				long timeOffset = timeOffsetValuePair.getKey();
				String valueString = timeOffsetValuePair.getValue();
				result += timeOffset + ", " + valueString + "\n";
			}
		}
		return result;
	}

	static String writeIoTDBRecord(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList,
			List<List<TSDataType>> typeList, List<List<Object>> valuesList) {
		String result = "";
		for (int i = 0; i < deviceIds.size(); i++) {
			result += String.format("deviceId, %s, time, %s, measurements, %s, types, %s, valueList, %s\n",
					deviceIds.get(i), times.get(i), measurementsList.get(i), typeList.get(i), valuesList.get(i));
		}
		return result;
	}

	public static void write(TransPacket transPacket, List<String> deviceIds, List<Long> times,
                             List<List<String>> measurementsList, List<List<TSDataType>> typeList, List<List<Object>> valuesList)
			throws IOException {
		Random random = new Random();
		long code = random.nextLong();
		if (code < 0) {
			code = -code;
		}
		String fileName = Config.USER_NAME + code;
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
		file.createNewFile();
		FileWriter writer = new FileWriter(file, true);
		String transPacketString = writeTransPacket(transPacket);
		writer.write(transPacketString);
		writer.write(">>>>>>>>>>>>>>>>>\n");
		String IoTDBRecordString = writeIoTDBRecord(deviceIds, times, measurementsList, typeList, valuesList);
		writer.write(IoTDBRecordString);
		writer.flush();
		writer.close();
	}
}
