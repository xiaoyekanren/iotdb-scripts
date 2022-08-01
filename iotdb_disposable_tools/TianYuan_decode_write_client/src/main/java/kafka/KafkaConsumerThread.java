package kafka;

import cn.hutool.core.date.DateUnit;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ty.pub.ParsedPacketDecoder;
import ty.pub.TransPacket;
import util.CreateKafkaProducer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import static kafka.Config.TOPIC;
import static kafka.Config.timeThreshold;
import static run.Launcher.*;
import static util.Util.selectLast;

public class KafkaConsumerThread implements Runnable {

	private static String messGenerateTime = "MsgTime@long";
	private String customTime = "CustomTime@long";
	private String tmnlID1 = "TmnlID@string";
	private String tmnlID2 = "CustomTime@string";
	private KafkaStream<byte[], byte[]> stream;
	private int storageGroupCount = 50;
	private static Set<String> cache = new HashSet<>();

	private static final String GEN_TIME_TEXT = "gen_time";
	private static final String RECV_TIME_TEXT = "recv_time";
	private static final String TIME_TEXT = "time";
	private static final String END_TIME_TEXT = "end_time";

	private static final String PREFIX_WORK_STATUS = "work_status_";
	private static final String PREFIX_LOG = "log_";
	private static final String PREFIX_EVENT = "event_";
	private static final String PREFIX_POSITION = "position_";
	private static final String PREFIX_FAULT_HIST = "fault_hist_";
	private static final String PREFIX_FAULT_MATCH = "fault_match_";
	private static final String PREFIX_ALARM_MATCH = "alarm_match_";
	private static final String PREFIX_ALARM_HIST = "alarm_hist_";

	private static final String SUFFIX_WORK_STATUS = "_work_status";
	private static final String JSON_TEXT = "JSON";
	private static final String TYPE_TEXT = "TYPE";

	private static final String DOT = ".";

	Map<Long, Long> workStatusGenAbsTimeGenTimeMap = new HashMap<>();
	Map<Long, Long> workStatusGenAbsTimeRecvTimeMap = new HashMap<>();
	Map<Long, Long> logTimeRecvTimeMap = new HashMap<>();
	Map<Long, Long> faultHistGenTimeRecvTimeMap = new HashMap<>();
	Map<Long, Long> alarmHistGenTimeRecvTimeMap = new HashMap<>();

	String deviceId;
	long genTime, recvTime;
	RowsOfRecords rowsOfRecords;

	private Map<String, Long> map;
	private static KafkaProducer<String, byte[]> producer;

	private static BlockingQueue<Runnable> threadQueue = new LinkedBlockingQueue<Runnable>(100000);

	private static ExecutorService insertThreadPool = new ThreadPoolExecutor(150, 150, 0L, TimeUnit.MILLISECONDS,
			threadQueue, new CustomPolicy());


	private static SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");

	public KafkaConsumerThread(KafkaStream<byte[], byte[]> stream, int storageGroupCount) {
		this.stream = stream;
		this.storageGroupCount = storageGroupCount;

		map = new ConcurrentHashMap<>();
		producer = CreateKafkaProducer.gerProducer();
	}

	private static class CustomPolicy implements RejectedExecutionHandler {

		public CustomPolicy() {
		}

		@Override
		public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
			try {
				synchronized (r) {
					r.wait(10000);
				}
			} catch (InterruptedException e) {
				System.out.print(formatter.format(new Date(System.currentTimeMillis())));
				e.printStackTrace();
			}
		}
	}

	@Override
	public void run() {
		ParsedPacketDecoder packetDecoder = new ParsedPacketDecoder();
		for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
			try {
				byte[] uploadMessage = messageAndMetadata.message();

				TransPacket transPacket = packetDecoder.deserialize(null, uploadMessage);

				Map<String, Map<Long, String>> workStatusMap = transPacket.getWorkStatusMap();
				if (workStatusMap.isEmpty()) {
					continue;
				}

				Map<String, String> baseInfoMap = transPacket.getBaseInfoMap();
				String genTimeString = baseInfoMap.get(messGenerateTime);
				if (genTimeString == null) {
					genTimeString = baseInfoMap.get("TY_0001_02_10@long");
					if (genTimeString == null) {
						genTimeString = baseInfoMap.get("TY_0001_00_6@long");
						if (genTimeString == null) {
							genTimeString = baseInfoMap.get("MsgTime");
							if (genTimeString == null) {
								continue;
							}
						}
					}
				}

				// basic information of the package, including, genTime, recvTime and deviceId
				genTime = Long.parseLong(genTimeString);
				recvTime = transPacket.getTimestamp();
				String vclId = transPacket.getDeviceId();

				String terminalId = baseInfoMap.getOrDefault(tmnlID1, baseInfoMap.get(tmnlID2));
				terminalId = StrUtil.isEmpty(terminalId) ? transPacket.getDeviceId() : terminalId;
				deviceId = genDeviceId(p.getProperty("iotdb.username"), vclId, terminalId);

				if (timeFilter == 1) {
					if (Math.abs(recvTime - genTime) > timeThreshold) {
						Long mTime = map.get(deviceId);
						if (mTime == null) {
							mTime = selectLast(deviceId);
							map.put(deviceId, mTime);
						}
						if (genTime <= (mTime - DateUnit.DAY.getMillis() * 60)) {
							producer.send(new ProducerRecord<>(TOPIC, transPacket.getDeviceId(), uploadMessage));
							continue;
						}
					}

					map.put(deviceId, genTime);
				}

				if (iscci) {
					Map<Long, String> remove = workStatusMap.remove("TY_0002_00_1213@string");
					if (remove != null) {
						workStatusMap.clear();
						workStatusMap.put("TY_0002_00_1213@string", remove);
						transPacket.setWorkStatusMap(workStatusMap);
					} else {
						continue;
					}
				}

				for (Entry<String, String> entry : baseInfoMap.entrySet()) {
					if (entry.getKey().startsWith("CustomTime") || entry.getValue() == null
							|| !entry.getKey().contains("@")) {
						continue;
					}
					transPacket.addWorkStatus(entry.getKey(), 0L, entry.getValue());
				}

				writeTransPacket(transPacket);

			} catch (Exception e) {
				System.out.print(formatter.format(new Date(System.currentTimeMillis())));
				e.printStackTrace();
			}
		}
	}

	private void writeTransPacket(TransPacket transPacket)
			throws StatementExecutionException, IoTDBConnectionException, ParseException {

		rowsOfRecords = new RowsOfRecords(deviceId);
		writeWorkMap(transPacket.getWorkStatusMap());

		if (!rowsOfRecords.isEmpty()) {
			rowsOfRecords.organize();
			insert(rowsOfRecords);
		}
	}

	/**
	 * A work status map contains multiple blocks. Each block corresponds to a
	 * single work status. This methods organizes all blocks of data to rows of
	 * records of IoTDB.
	 */
	private void writeWorkMap(Map<String, Map<Long, String>> workStatusMap)
			throws StatementExecutionException, IoTDBConnectionException, ParseException {

		// initialize recv time columns shared by multiple work status
		workStatusGenAbsTimeGenTimeMap.clear();
		workStatusGenAbsTimeRecvTimeMap.clear();
		logTimeRecvTimeMap.clear();
		faultHistGenTimeRecvTimeMap.clear();
		alarmHistGenTimeRecvTimeMap.clear();

		for (Entry<String, Map<Long, String>> workStatusData : workStatusMap.entrySet()) {
			String[] arr = workStatusData.getKey().split("@");

			if (arr.length < 2) {
				continue;
			}

			String workStatusName = arr[0];
			String type = arr[1];

			if (workStatusData.getValue() == null) {
				continue;
			}

			// write a single block of data.
			// data inside a block belong to a single work status
			writeSingleBlock(workStatusMap, workStatusName, type, workStatusData.getValue());
		}

		// write workStatusGenAbsTimeGenTimeMap
		if (!workStatusGenAbsTimeGenTimeMap.isEmpty()) {
			writeGenTime(PREFIX_WORK_STATUS);
		}

		// write genAbsTimeRecvTimeMap
		if (!workStatusGenAbsTimeRecvTimeMap.isEmpty()) {
			writeRecvTime(PREFIX_WORK_STATUS);
		}
		// write logTimeRecvTimeMap
		if (!logTimeRecvTimeMap.isEmpty()) {
			writeRecvTime(PREFIX_LOG);
		}
		// write faultTimeRecvTimeMap
		if (!faultHistGenTimeRecvTimeMap.isEmpty()) {
			writeRecvTime(PREFIX_FAULT_HIST);
		}
		// write alarmHistGenTimeRecvTimeMap
		if (!alarmHistGenTimeRecvTimeMap.isEmpty()) {
			writeRecvTime(PREFIX_ALARM_HIST);
		}
	}

	private void writeGenTime(String prefix) throws StatementExecutionException, IoTDBConnectionException {
		Map<Long, Long> genTimeMap;
		switch (prefix) {
			case PREFIX_WORK_STATUS:
				genTimeMap = workStatusGenAbsTimeGenTimeMap;
				break;
			default:
				return;
		}
		for (Entry<Long, Long> pair : genTimeMap.entrySet()) {
			if (rowsOfRecords.shouldInsertAfterAddRecords(pair.getKey(),
					Collections.singletonList(prefix + GEN_TIME_TEXT), Collections.singletonList(TSDataType.INT64),
					Collections.singletonList(pair.getValue()))) {
				insert(rowsOfRecords);
				rowsOfRecords.reset();
			}
		}
	}

	/**
	 * organize recvTime of workstatus, logs, faultHist, alarmHist data according to
	 * given prefix
	 *
	 * @param prefix
	 */
	private void writeRecvTime(String prefix) throws StatementExecutionException, IoTDBConnectionException {
		Map<Long, Long> recvTimeMap;
		switch (prefix) {
			case PREFIX_WORK_STATUS:
				recvTimeMap = workStatusGenAbsTimeRecvTimeMap;
				break;
			case PREFIX_LOG:
				recvTimeMap = logTimeRecvTimeMap;
				break;
			case PREFIX_FAULT_HIST:
				recvTimeMap = faultHistGenTimeRecvTimeMap;
				break;
			case PREFIX_ALARM_HIST:
				recvTimeMap = alarmHistGenTimeRecvTimeMap;
				break;
			default:
				return;
		}
		for (Entry<Long, Long> pair : recvTimeMap.entrySet()) {
			if (rowsOfRecords.shouldInsertAfterAddRecords(pair.getKey(),
					Collections.singletonList(prefix + RECV_TIME_TEXT), Collections.singletonList(TSDataType.INT64),
					Collections.singletonList(pair.getValue()))) {
				insert(rowsOfRecords);
				rowsOfRecords.reset();
			}
		}
	}

	/**
	 * To organize the data in a single block to rows of records of IoTDB
	 */
	private void writeSingleBlock(Map<String, Map<Long, String>> workStatusMap, String blockName, String dataTypeString,
			Map<Long, String> timeOffsetAndValues)
			throws StatementExecutionException, IoTDBConnectionException, ParseException {

		if (BlockName.LOG_WORKSTATUS_NAMES.contains(blockName)) {
			writeLogData(workStatusMap, dataTypeString, blockName, timeOffsetAndValues);
		} else if (BlockName.FAULT_HIST_WORKSTATUS_NAMES.contains(blockName)) {
			writeFaultHistData(dataTypeString, blockName, timeOffsetAndValues);
		} else if (BlockName.ALARM_HIST_WORKSTATUS_NAMES.contains(blockName)) {
			writeAlarmHistData(dataTypeString, blockName, timeOffsetAndValues);
		} else if (BlockName.POSITION_WORKSTATUS_NAMES.contains(blockName)) {
			// position data will be treated as normal work status data as well
			// so that work status and position can be aligned
			writeWorkStatusData(dataTypeString, PREFIX_WORK_STATUS + blockName, timeOffsetAndValues);
			writePositionData(dataTypeString, blockName, timeOffsetAndValues);
		} else if (BlockName.EVENT_WORKSTATUS_NAMES.contains(blockName)) {
			// TODO
			writeEventData(dataTypeString, blockName, timeOffsetAndValues);
			// } else if (BlockName.ALARM_MATCH_WORKSTATUS_NEW.contains(blockName)) {
			// // TODO
			// writeAlarmMatchData(dataTypeString, blockName, timeOffsetAndValues);
			// } else if (BlockName.FAULT_MATCH_WORKSTATUS_NAME.contains(blockName)) {
			// writeFaultMatchData(dataTypeString, blockName, timeOffsetAndValues);
			// // TODO
		} else {
			writeWorkStatusData(dataTypeString, blockName, timeOffsetAndValues);
		}
	}

	/**
	 * Organize a block, where the data belongs to a single workStatus. Different
	 * workStatus share the same recvTime, so a workStatusGenAbsTimeRecvTimeMap is
	 * maintained. The timestamp is absolute time. Each workstatus corresponds to 2
	 * timeseries: one for genTime, one for workstatus value.
	 */
	private void writeWorkStatusData(String dataTypeString, String workStatusName,
			Map<Long, String> timeOffsetsAndValuesMap) throws StatementExecutionException, IoTDBConnectionException {
		// position name -> position name + "_work_status"
		if (BlockName.POSITION_WORKSTATUS_NAMES.contains(workStatusName)) {
			workStatusName += SUFFIX_WORK_STATUS;
		}
		if (workStatusName.equals("TY_0002_00_17") || workStatusName.equals("TY_0002_00_18")
				|| workStatusName.equals("TY_0002_00_19") || workStatusName.equals("TY_0002_00_20")
				|| workStatusName.equals("TY_0002_00_24")) {
			dataTypeString = "double";
		}
		switch (dataTypeString) {
			case "string":
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					long genAbsTime = item.getKey() + genTime;
					String valueString = item.getValue();
					if (rowsOfRecords.shouldInsertAfterAddRecords(genAbsTime,
							new ArrayList<>(Collections.singletonList(workStatusName)), // "TY_0001_00_3"
							new ArrayList<>(Collections.singletonList(TSDataType.TEXT)), // TEXT
							new ArrayList<>(Collections.singletonList(addStrValue(valueString))))) { // string value
						insert(rowsOfRecords);
						rowsOfRecords.reset();
					}
					workStatusGenAbsTimeGenTimeMap.put(genAbsTime, genTime);
					workStatusGenAbsTimeRecvTimeMap.put(genAbsTime, recvTime);
				}
				break;
			case "int":
			case "long":
				// if (workStatusName.equals("TY_0002_00_17")
				// || workStatusName.equals("TY_0002_00_18")
				// || workStatusName.equals("TY_0002_00_19")
				// || workStatusName.equals("TY_0002_00_20")
				// || workStatusName.equals("TY_0002_00_24")
				// ) {
				// System.out.println(workStatusName + "is long ");
				// }
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					try {
						long genAbsTime = item.getKey() + genTime;
						String valueString = item.getValue();
						if (valueString != null && valueString.contains(".")) {
							valueString = valueString.substring(0, valueString.indexOf("."));
						}
						if (rowsOfRecords.shouldInsertAfterAddRecords(genAbsTime,
								new ArrayList<>(Collections.singletonList(workStatusName)), // "TY_0001_00_3"// TODO
																							// name
								new ArrayList<>(Collections.singletonList(TSDataType.INT64)), // INT64
								new ArrayList<>(Collections.singletonList(Long.parseLong(valueString)))) // long value
						) {
							insert(rowsOfRecords);
							rowsOfRecords.reset();
						}
						workStatusGenAbsTimeGenTimeMap.put(genAbsTime, genTime);
						workStatusGenAbsTimeRecvTimeMap.put(genAbsTime, recvTime);
					} catch (NumberFormatException e) {
						System.out.print(formatter.format(new Date(System.currentTimeMillis())));
						e.printStackTrace();
					}
				}
				break;
			case "float":
			case "double":
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					try {
						long genAbsTime = item.getKey() + genTime;
						String valueString = item.getValue();
						double valueDouble;
						// to get the double value
						if (workStatusName.equals("TY_0002_00_4_GeoAltitude")) {
							// a special case where the data is read from json object, instead of
							// valueString
							JSONArray jsonArray = JSONArray.parseArray(valueString);
							if (jsonArray.size() == 0) {
								System.out.println("@^^ERROR");
								continue;
							}
							// the last JSON object is selected
							valueDouble = jsonArray.getJSONObject(jsonArray.size() - 1).getDoubleValue("Altitude");
						} else {
							valueDouble = Double.parseDouble(valueString);
						}
						if (rowsOfRecords.shouldInsertAfterAddRecords(genAbsTime,
								new ArrayList<>(Collections.singletonList(workStatusName)), // "TY_0001_00_3"// TODO to
																							// check
								new ArrayList<>(Collections.singletonList(TSDataType.DOUBLE)), // DOUBLE
								new ArrayList<>(Collections.singletonList(valueDouble)))) {
							insert(rowsOfRecords);
							rowsOfRecords.reset();
						}
						workStatusGenAbsTimeGenTimeMap.put(genAbsTime, genTime);
						workStatusGenAbsTimeRecvTimeMap.put(genAbsTime, recvTime);
					} catch (NumberFormatException e) {
						System.out.print(formatter.format(new Date(System.currentTimeMillis())));
						e.printStackTrace();
					}
				}
				break;
			case "geo":// position data will be treated as normal work status data for aligning
			case "gps":
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					long genAbsTime = item.getKey() + genTime;
					String valueString = item.getValue();
					JSONArray jsonArray = JSON.parseArray(valueString);
					valueString = jsonArray.getJSONObject(jsonArray.size() - 1).toJSONString();// select the last one
					if (rowsOfRecords.shouldInsertAfterAddRecords(genAbsTime,
							new ArrayList<>(Collections.singletonList(workStatusName)), // "TY_0002_00_4_Geo_State"
							new ArrayList<>(Collections.singletonList(TSDataType.TEXT)), // TEXT
							new ArrayList<>(Collections.singletonList(addStrValue(valueString))))) {
						insert(rowsOfRecords);
						rowsOfRecords.reset();
					}
					workStatusGenAbsTimeGenTimeMap.put(genAbsTime, genTime);
					workStatusGenAbsTimeRecvTimeMap.put(genAbsTime, recvTime);
				}
			case "map":// position data will be treated as normal work status for aligning
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					long genAbsTime = item.getKey() + genTime;
					JSONArray jsonArray = JSON.parseArray(item.getValue());
					String valueString = jsonArray.getJSONObject(jsonArray.size() - 1).toJSONString();
					if (rowsOfRecords.shouldInsertAfterAddRecords(genAbsTime,
							new ArrayList<>(Collections.singletonList(workStatusName)),
							new ArrayList<>(Collections.singletonList(TSDataType.TEXT)),
							new ArrayList<>(Collections.singletonList(addStrValue(valueString))))) {
						insert(rowsOfRecords);
						rowsOfRecords.reset();
					}
					workStatusGenAbsTimeGenTimeMap.put(genAbsTime, genTime);
					workStatusGenAbsTimeRecvTimeMap.put(genAbsTime, recvTime);
				}
			default:
				break;
		}
	}

	// private void writeAlarmMatchData(String dataTypeString, String
	// alarmMatchName,
	// Map<Long, String> timeOffsetsAndValuesMap) {
	// switch (dataTypeString) {
	// case "map":
	// for (Map.Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
	// long genAbsTime = item.getKey() + genTime;
	// String valueString = item.getValue();
	// JSONArray jsonArray = JSON.parseArray(valueString);
	// int curNumber = 0;
	// for (int i = 0; i < jsonArray.size(); i++) {
	// JSONObject jsonObject = jsonArray.getJSONObject(i);
	// long alarmCode = jsonObject.getIntValue("alarmTypeId");
	// //TODO 获得警报的生成时间和解除时间
	// long startTime = genTime;
	// long fixTime = recvTime;
	// if (recvTime == -1) {// the alarm is not fixed
	// rowsOfRecords.addRecords(deviceId, startTime,
	// new ArrayList<>(Arrays.asList(
	// alarmMatchName + "_code", // "TY_0001_00_45_Alarm_code"
	// alarmMatchName)), // "TY_0001_00_45_Alarm"
	// new ArrayList<>(Arrays.asList(
	// TSDataType.INT64, // INT64
	// TSDataType.TEXT)), // TEXT
	// new ArrayList<>(Arrays.asList(
	// alarmCode, // The alarm code
	// addStrValue(jsonObject.toJSONString())))); // the alarm JSON
	// } else { // the alarm is fixed
	// rowsOfRecords.addRecords(deviceId, startTime,
	// new ArrayList<>(Arrays.asList(
	// alarmMatchName + "_fix_time" // "TY_0001_00_45_Alarm_fix_time"
	// )),
	// new ArrayList<>(Arrays.asList(
	// TSDataType.INT64)), // "INT64"
	// new ArrayList<>(
	// Arrays.asList(startTime, alarmCode,
	// addStrValue(jsonObject.toJSONString()))));
	// }
	// }
	// }
	// break;
	// default:
	// break;
	// }
	// }

	// private void writeFaultMatchData(String dataTypeString, String
	// faultMatchName,
	// Map<Long, String> timeOffsetsAndValuesMap) {
	// switch (dataTypeString) {
	// case "map":
	// for (Map.Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
	// long genAbsTime = item.getKey() + genTime;
	// String valueString = item.getValue();
	// JSONArray jsonArray = JSON.parseArray(valueString);
	// for (int i = 0; i < jsonArray.size(); i++) {
	// JSONObject jsonObject = jsonArray.getJSONObject(i);
	// long faultCode =
	// jsonObject.getIntValue("MsgFA_SPN") * 100 +
	// jsonObject.getIntValue("MsgFA_FMI");
	// //TODO 获得故障的生成时间和解除时间
	// long startTime = 0;
	// long fixTime = 0;
	// rowsOfRecords.addRecords(deviceId, fixTime, new ArrayList<>(
	// Arrays.asList(faultMatchName + "_startTime", faultMatchName + "_code",
	// faultMatchName)),
	// new ArrayList<>(Arrays
	// .asList(TSDataType.INT64, TSDataType.INT64, TSDataType.TEXT)),
	// new ArrayList<>(
	// Arrays.asList(startTime, faultCode,
	// addStrValue(jsonObject.toJSONString()))));
	// }
	// }
	// break;
	// default:
	// break;
	// }
	// }

	private void writeEventData(String dataTypeString, String eventName, Map<Long, String> timeOffsetsAndValuesMap)
			throws StatementExecutionException, IoTDBConnectionException {
		switch (dataTypeString) {
			case "map":
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					String valueString = item.getValue();
					JSONArray jsonArray = JSON.parseArray(valueString);
					for (int i = 0; i < jsonArray.size(); i++) {
						JSONObject jsonObject = jsonArray.getJSONObject(i);
						long startTime = Long.parseLong(jsonObject.remove("ERC_StartTime").toString());
						long endTime = Long.parseLong(jsonObject.remove("ERC_EndTime").toString());
						valueString = jsonObject.toJSONString();
						// for TY_0002_00_705_gather, the event type is 705
						long eventType = Long.parseLong(eventName.split("_")[3]);
						if (rowsOfRecords.shouldInsertAfterAddRecords(startTime,
								new ArrayList<>(Arrays.asList(PREFIX_EVENT + END_TIME_TEXT, // "event_end_time"
										PREFIX_EVENT + RECV_TIME_TEXT, // "event_recv_time"
										PREFIX_EVENT + JSON_TEXT, // "event_JSON"
										PREFIX_EVENT + GEN_TIME_TEXT, // "event_gen_time"
										PREFIX_EVENT + TYPE_TEXT)), // "event_type"
								new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
										TSDataType.INT64, // INT64
										TSDataType.TEXT, // TEXT
										TSDataType.INT64, // INT64
										TSDataType.INT64)), // INT64
								new ArrayList<>(Arrays.asList(endTime, // end time
										recvTime, // recv time
										addStrValue(valueString), // JSON string
										genTime, // genTime
										eventType))) // eventType
						) {
							insert(rowsOfRecords);
							rowsOfRecords.reset();
						}
					}
				}
				break;
			default:
				break;
		}
	}

	private void writePositionData(String dataTypeString, String positionName,
			Map<Long, String> timeOffsetsAndValuesMap)
			throws StatementExecutionException, IoTDBConnectionException, ParseException {
		switch (dataTypeString) {
			case "double":
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					long genAbsTime = item.getKey() + genTime;
					JSONArray jsonArray = JSON.parseArray(item.getValue());
					for (int i = 0, n = jsonArray.size(); i < n; i++) {
						JSONObject jsonObject = jsonArray.getJSONObject(i);
						long positionTime = Long.parseLong(jsonObject.remove("PstnTime").toString());
						double altitude = jsonArray.getJSONObject(jsonArray.size() - 1).getDouble("Altitude");
						if (rowsOfRecords.shouldInsertAfterAddRecords(positionTime,
								new ArrayList<>(Arrays.asList(PREFIX_POSITION + GEN_TIME_TEXT, // "position_gen_time"
										PREFIX_POSITION + RECV_TIME_TEXT, // "position_recv_time"
										positionName)), // must be "TY_0002_00_4_GeoAltitude"
								new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
										TSDataType.INT64, // INT64
										TSDataType.DOUBLE)), // DOUBLE
								new ArrayList<>(Arrays.asList(genAbsTime, // genTime, which is equal to genAbsTime
										recvTime, // recv time
										altitude))) // altitude
						) {
							insert(rowsOfRecords);
							rowsOfRecords.reset();
						}
					}
				}
			case "gps":
			case "geo":
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					long genAbsTime = item.getKey() + genTime;
					String valueString = item.getValue();
					JSONArray jsonArray = JSON.parseArray(valueString);
					for (int i = 0, n = jsonArray.size(); i < n; i++) {
						JSONObject jsonObject = jsonArray.getJSONObject(i);
						long positionTime = Long.parseLong(jsonObject.remove("PstnTime").toString());
						double la = Double.parseDouble(jsonObject.remove("La").toString());
						double lo = Double.parseDouble(jsonObject.remove("Lo").toString());
						if (rowsOfRecords.shouldInsertAfterAddRecords(positionTime,
								new ArrayList<>(Arrays.asList(PREFIX_POSITION + GEN_TIME_TEXT, // "position_gen_time"
										PREFIX_POSITION + RECV_TIME_TEXT, // "position_recv_time"
										positionName + "_La", // "TY_0002_00_4_GeoAdt_La"
										positionName + "_Lo", // "TY_0002_00_4_GeoAdt_Lo"
										positionName)), // "TY_0002_00_4_GeoAdt"
								new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
										TSDataType.INT64, // INT64
										TSDataType.DOUBLE, // DOUBLE
										TSDataType.DOUBLE, // DOUBLE
										TSDataType.TEXT)), // TEXT
								new ArrayList<>(Arrays.asList(genTime, // genTime, equals to genAbsTime
										recvTime, // recv time
										la, // latitude
										lo, // longitude
										addStrValue(jsonObject.toJSONString()))))// JSON string
						) {
							insert(rowsOfRecords);
							rowsOfRecords.reset();
						}
					}
				}
				break;
			case "string":// 神钢 GB4
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				simpleDateFormat.setTimeZone(TIME_ZONE);
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					long genAbsTime = item.getKey() + genTime;
					JSONObject jsonObject = JSON.parseObject(item.getValue());
					String s = jsonObject.getString("PstnTime");
					long positionTime;
					try {
						positionTime = simpleDateFormat.parse(s).getTime();
					} catch (Exception e) {
						System.out.println("cannot parse PstnTime " + positionName);
						continue;
					}
					double la = Double.parseDouble(jsonObject.getString("La"));
					double lo = Double.parseDouble(jsonObject.getString("Lo"));
					if (rowsOfRecords.shouldInsertAfterAddRecords(positionTime,
							new ArrayList<>(Arrays.asList(PREFIX_POSITION + GEN_TIME_TEXT, // "position_gen_time"
									PREFIX_POSITION + RECV_TIME_TEXT, // "position_recv_time"
									positionName + "_La", // "TC_0001_00_1_La"
									positionName + "_Lo", // "TC_0001_00_1_Lo"
									positionName)), // "TC_0001_00_1"
							new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
									TSDataType.INT64, // INT64
									TSDataType.DOUBLE, // DOUBLE
									TSDataType.DOUBLE, // DOUBLE
									TSDataType.TEXT)), // DOUBLE
							new ArrayList<>(Arrays.asList(genAbsTime, // genTime, which is equal to genAbsTime
									recvTime, // recv time
									la, // latitude
									lo, // longitude
									addStrValue(jsonObject.toJSONString())))) // altitude
					) {
						insert(rowsOfRecords);
						rowsOfRecords.reset();
					}
				}
				break;
			case "map":
				// "TY_0002_00_4_GeoAltitude_State" contains no La and Lo data, so there's no
				// way to
				// generate La and Lo timeseries.
				if (positionName.equals("TY_0002_00_4_GeoAltitude_State")) {
					for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
						long genAbsTime = item.getKey() + genTime;
						JSONArray jsonArray = JSON.parseArray(item.getValue());
						for (int i = 0, n = jsonArray.size(); i < n; i++) {
							JSONObject jsonObject = jsonArray.getJSONObject(i);
							long positionTime = Long.parseLong(jsonObject.remove("PstnTime").toString());
							String valueString = jsonObject.toJSONString();
							if (rowsOfRecords.shouldInsertAfterAddRecords(positionTime,
									new ArrayList<>(Arrays.asList(PREFIX_POSITION + GEN_TIME_TEXT, // "position_gen_time"
											PREFIX_POSITION + RECV_TIME_TEXT, // "position_recv_time"
											positionName)), // "TY_0002_00_4_GeoAltitude_State"
									new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
											TSDataType.INT64, // INT64
											TSDataType.TEXT)), // TEXT
									new ArrayList<>(Arrays.asList(genAbsTime, // gentime, which is equal to genAbsTime
											recvTime, // recv time
											addStrValue(valueString)))) // JSON string
							) {
								insert(rowsOfRecords);
								rowsOfRecords.reset();
							}
						}
					}
				} else {
					for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
						long genAbsTime = item.getKey() + genTime;
						JSONArray jsonArray = JSON.parseArray(item.getValue());
						for (int i = 0, n = jsonArray.size(); i < n; i++) {
							JSONObject jsonObject = jsonArray.getJSONObject(i);
							long positionTime = Long.parseLong(jsonObject.remove("PstnTime").toString());
							double la = Double.parseDouble(jsonObject.remove("La").toString());
							double lo = Double.parseDouble(jsonObject.remove("Lo").toString());
							if (rowsOfRecords.shouldInsertAfterAddRecords(positionTime,
									new ArrayList<>(Arrays.asList(PREFIX_POSITION + GEN_TIME_TEXT, // "position_gen_time"
											PREFIX_POSITION + RECV_TIME_TEXT, // "position_recv_time"
											positionName + "_La", // "TY_0002_00_4_GeoAdt_State_La"
											positionName + "Lo", // "TY_0002_00_4_GeoAdt_State_Lo"
											positionName)), // "TY_0002_00_4_GeoAdt_State"
									new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
											TSDataType.INT64, // INT64
											TSDataType.DOUBLE, // DOUBLE
											TSDataType.DOUBLE, // DOUBLE
											TSDataType.TEXT)), // TEXT
									new ArrayList<>(Arrays.asList(genAbsTime, // gentime, equals to genAbsTime
											recvTime, // recv time
											la, // latitude
											lo, // longitude
											addStrValue(jsonObject.toJSONString())))) // JSON string
							) {
								insert(rowsOfRecords);
								rowsOfRecords.reset();
							}
						}
					}
				}

				break;
			default:
				break;
		}
	}

	private void writeAlarmHistData(String dataTypeString, String alarmName, Map<Long, String> timeOffsetsAndValuesMap)
			throws StatementExecutionException, IoTDBConnectionException {
		switch (dataTypeString) {
			case "int":
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					long genAbsTime = item.getKey() + genTime;
					String valueString = item.getValue();
					if (valueString != null && valueString.contains(".")) {
						valueString = valueString.substring(0, valueString.indexOf("."));
					}
					if (rowsOfRecords.shouldInsertAfterAddRecords(genAbsTime,
							new ArrayList<>(Arrays.asList(PREFIX_ALARM_HIST + GEN_TIME_TEXT, // "alarm_hist_gen_time"
									alarmName)), // "TY_0002_00_60"
							new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
									TSDataType.INT64)), // INT64
							new ArrayList<>(Arrays.asList(genTime, // gen_time
									Long.parseLong(valueString)))) // an integer
					) {
						insert(rowsOfRecords);
						rowsOfRecords.reset();
					}
					alarmHistGenTimeRecvTimeMap.put(genAbsTime, recvTime);
				}
				break;
			default:
				break;
		}

	}

	private void writeFaultHistData(String dataTypeString, String faultHistName,
			Map<Long, String> timeOffsetsAndValuesMap) throws StatementExecutionException, IoTDBConnectionException {
		switch (dataTypeString) {
			case "string":
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					long genAbsTime = item.getKey() + genTime;
					String valueString = item.getValue();
					if (rowsOfRecords.shouldInsertAfterAddRecords(genAbsTime,
							new ArrayList<>(Arrays.asList(PREFIX_FAULT_HIST + GEN_TIME_TEXT, // "fault_hist_gen_time"
									faultHistName)), // "TY_0002_00_8"
							new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
									TSDataType.TEXT)), // TEXT
							new ArrayList<>(Arrays.asList(genTime, // gen time
									addStrValue(valueString)))) // JSON string
					) {
						insert(rowsOfRecords);
						rowsOfRecords.reset();
					}
					faultHistGenTimeRecvTimeMap.put(genAbsTime, recvTime);
				}
				break;
			default:
				break;
		}
	}

	private void writeLogData(Map<String, Map<Long, String>> workStatusMap, String dataTypeString, String logName,
			Map<Long, String> timeOffsetsAndValuesMap) throws StatementExecutionException, IoTDBConnectionException {
		switch (dataTypeString) {
			case "string":
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					long logTime = item.getKey() + genTime;
					String valueString = item.getValue();
					if (rowsOfRecords.shouldInsertAfterAddRecords(logTime,
							new ArrayList<>(Arrays.asList(PREFIX_LOG + TIME_TEXT, // "log_time"
									logName)), // "TY_0002_00_162"
							new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
									TSDataType.TEXT)), // TEXT
							new ArrayList<>(Arrays.asList(genTime, // genTime
									addStrValue(valueString)))) // JSON string
					) {
						insert(rowsOfRecords);
						rowsOfRecords.reset();
					}
					logTimeRecvTimeMap.put(logTime, recvTime);
				}
				break;
			case "int":
			case "long":
				for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
					long logTime = item.getKey() + genTime;
					String valueString = item.getValue();
					if (valueString != null && valueString.contains(".")) {
						valueString = valueString.substring(0, valueString.indexOf("."));
					}
					if (rowsOfRecords.shouldInsertAfterAddRecords(logTime,
							new ArrayList<>(Arrays.asList(PREFIX_LOG + TIME_TEXT, // "log_time"
									logName)), // "TY_0002_00_167"
							new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
									TSDataType.INT64)), // INT64
							new ArrayList<>(Arrays.asList(genTime, // genTime
									Long.parseLong(valueString)))) // JSON string
					) {
						insert(rowsOfRecords);
						rowsOfRecords.reset();
					}
					logTimeRecvTimeMap.put(logTime, recvTime);
				}
				break;
			case "map":
				long logTime;
				switch (logName) {
					// for "TY_0002_00_732_gather" and "TY_0002_00_733_gather", the way to get log
					// time is
					// different
                    case "TY_0002_00_173_gather":
                        for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
                            Map<Long, String> map = workStatusMap.get("TY_0002_00_731@long");
                            if (map == null) {
                                continue;
                            }
                            logTime = Long.parseLong(map.get(0L));
                            JSONArray jsonArray = JSON.parseArray(item.getValue());
                            for (int i = 0; i < jsonArray.size(); i++) {
                                JSONObject jsonObject = jsonArray.getJSONObject(i);
                                if (rowsOfRecords.shouldInsertAfterAddRecords(logTime,
                                        new ArrayList<>(Arrays.asList(PREFIX_LOG + TIME_TEXT, // "log_time"
                                                logName)), // "TY_0002_00_732_gather"
                                        new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
                                                TSDataType.TEXT)), // TEXT
                                        new ArrayList<>(Arrays.asList(genTime, // gen_time
                                                addStrValue(jsonObject.toJSONString()))))// JSON string
                                        ) {
                                    insert(rowsOfRecords);
                                    rowsOfRecords.reset();
                                }
                                logTimeRecvTimeMap.put(logTime, recvTime);
                            }
                        }
                        break;
                    case "TY_0002_00_732_gather":
						for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
							// get log time
							Map<Long, String> orDefault = workStatusMap.getOrDefault("TY_0002_00_732@long",
									workStatusMap.get("TY_0002_00_920@long"));
							if (orDefault == null) {
								return;
							}
							logTime = Long.parseLong(orDefault.get(0L));
							JSONArray jsonArray = JSON.parseArray(item.getValue());
							for (int i = 0; i < jsonArray.size(); i++) {
								JSONObject jsonObject = jsonArray.getJSONObject(i);
								if (rowsOfRecords.shouldInsertAfterAddRecords(logTime,
										new ArrayList<>(Arrays.asList(PREFIX_LOG + TIME_TEXT, // "log_time"
												logName)), // "TY_0002_00_732_gather"
										new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
												TSDataType.TEXT)), // TEXT
										new ArrayList<>(Arrays.asList(genTime, // gen_time
												addStrValue(jsonObject.toJSONString()))))// JSON string
								) {
									insert(rowsOfRecords);
									rowsOfRecords.reset();
								}
								logTimeRecvTimeMap.put(logTime, recvTime);
							}
						}
						break;
					case "TY_0002_00_733_gather":
						for (Entry<Long, String> item : timeOffsetsAndValuesMap.entrySet()) {
							logTime = Long.parseLong(workStatusMap.get("TY_0002_00_733@long").get(0L));
							JSONArray jsonArray = JSON.parseArray(item.getValue());
							for (int i = 0; i < jsonArray.size(); i++) {
								JSONObject jsonObject = jsonArray.getJSONObject(i);
								if (rowsOfRecords.shouldInsertAfterAddRecords(logTime,
										new ArrayList<>(Arrays.asList(PREFIX_LOG + TIME_TEXT, // "log_time"
												logName)), // "TY_0002_00_733_gather"
										new ArrayList<>(Arrays.asList(TSDataType.INT64, // INT64
												TSDataType.TEXT)), // TEXT
										new ArrayList<>(Arrays.asList(genTime, // genTime
												addStrValue(jsonObject.toJSONString())))) // JSON String
								) {
									insert(rowsOfRecords);
									rowsOfRecords.reset();
								}
								logTimeRecvTimeMap.put(logTime, recvTime);
							}
						}
						break;
					default:
						break;
				}
				break;
			default:
				break;
		}
	}

	private String genDeviceId(String user, String vclId, String tmnlId) {
		// String countStr = "50";
		// int storageGroupCount = Convert.toInt(countStr);
		// return "root" + DOT + user + DOT + "trans" + DOT + vclId + DOT + tmnlId;

		// storage group name
		String group = padLeft(String.valueOf(Long.parseLong(vclId) % storageGroupCount),
				p.getProperty("iotdb.storageGroupCount").length(), '0');
		return deviceId = "root" + DOT + user + DOT + "trans" + DOT + group + DOT + vclId + DOT + tmnlId;
	}

	private static void insert(RowsOfRecords rowsOfRecords)
			throws StatementExecutionException, IoTDBConnectionException {
		if (rowsOfRecords.getDeviceId().split("\\.\\.").length == 1) {
			insertThreadPool.submit(new SessionInsertThread(rowsOfRecords));
		} else {
			System.out.println("invalidate deviceId: " + rowsOfRecords.getDeviceId());
		}

		// sessionPool.testInsertRecords(rowsOfRecords.getDeviceIdList(),
		// rowsOfRecords.getTimestampList(),
		// rowsOfRecords.getMeasurementsList(), rowsOfRecords.getTypeLists(),
		// rowsOfRecords.getValueLists());
	}

	private static String addStrValue(String value) {
		// return "\"" + value + "\"";
		// 之前版本的iotdb写入string类型需要在前后加双引号，后续版本不需要
		return value;
	}

}
