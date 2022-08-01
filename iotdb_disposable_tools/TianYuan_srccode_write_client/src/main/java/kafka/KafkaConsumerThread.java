package kafka;

import cn.hutool.core.convert.Convert;
import com.alibaba.fastjson.JSON;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import ty.pub.RawDataPacket;
import ty.pub.RawPacketDecoder;
import uitl.Decompression;

import javax.xml.bind.DatatypeConverter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import static run.Launcher.padLeft;

public class KafkaConsumerThread implements Runnable {


	private static String messGenerateTime = "MsgTime@long";
	private String customTime = "CustomTime@long";
	private String tmnlID = "TmnlID@string";
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

	private static BlockingQueue<Runnable> threadQueue = new LinkedBlockingQueue<Runnable>(100000);

	private static ExecutorService insertThreadPool = new ThreadPoolExecutor(150, 150, 0L, TimeUnit.MILLISECONDS,
			threadQueue, new CustomPolicy());

	public KafkaConsumerThread(KafkaStream<byte[], byte[]> stream, int storageGroupCount) {
		this.stream = stream;
		this.storageGroupCount = storageGroupCount;
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
				e.printStackTrace();
			}
		}
	}

	@Override
	public void run() {
		// ParsedPacketDecoder packetDecoder = new ParsedPacketDecoder();
		RawPacketDecoder packetDecoder = new RawPacketDecoder();
		for (MessageAndMetadata<byte[], byte[]> consumerIterator : stream) {
			try {
				// long start = System.currentTimeMillis();
				byte[] uploadMessage = consumerIterator.message();
				// System.out.println(uploadMessage);


				RawDataPacket transPacket = packetDecoder.deserialize(null, uploadMessage);
				// System.out.print("decode time, " + (System.currentTimeMillis() - start) + ",
				// object size, "
				// + uploadMessage.length);


				writeRawDatePacket(transPacket);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void writeRawDatePacket(RawDataPacket rawDataPacket)
			throws StatementExecutionException, IoTDBConnectionException {
        String imei = "8000" + rawDataPacket.getIMEI();
        String source = DatatypeConverter.printHexBinary((byte[]) rawDataPacket.getPacketData());
		rawDataPacket.setPacketData(source);
		genTime = rawDataPacket.getTimestamp();
		recvTime = -1;
		deviceId = genDeviceId(imei);
		rowsOfRecords = new RowsOfRecords(deviceId);

        String s = JSON.toJSONString(rawDataPacket);

        String decomSource = null;
        try {
            RawDataPacket decomRawDatePacket = Decompression.decom(rawDataPacket, imei, source);
            decomSource = JSON.toJSONString(decomRawDatePacket);
        } catch (Exception e) {

        }


        if (rowsOfRecords.shouldInsertAfterAddRecords(genTime,
                new ArrayList<>(Collections.singletonList("TY_0001_Raw_Packet")), // "TY_0001_00_3"
                new ArrayList<>(Collections.singletonList(TSDataType.TEXT)), // TEXT
                new ArrayList<>(Collections.singletonList(s)))) { // string value
            insert(rowsOfRecords);
            rowsOfRecords.reset();
        }


        if (decomSource != null) {
            if (rowsOfRecords.shouldInsertAfterAddRecords(genTime,
                    new ArrayList<>(Collections.singletonList("TY_0001_Raw_Packet_Dec")), // "TY_0001_00_3"
                    new ArrayList<>(Collections.singletonList(TSDataType.TEXT)), // TEXT
                    new ArrayList<>(Collections.singletonList(decomSource)))) { // string value
                insert(rowsOfRecords);
                rowsOfRecords.reset();
            }
        }

        if (rowsOfRecords.shouldInsertAfterAddRecords(genTime,
                new ArrayList<>(Collections.singletonList("work_status_recv_time")), // "TY_0001_00_3"
                new ArrayList<>(Collections.singletonList(TSDataType.INT64)), // TEXT
                new ArrayList<>(Collections.singletonList(genTime)))) { // string value
            insert(rowsOfRecords);
            rowsOfRecords.reset();
        }



        if (!rowsOfRecords.isEmpty()) {
			rowsOfRecords.organize();
			insert(rowsOfRecords);
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




	private String genDeviceId(String imei) {

        try {
            long l = Long.parseLong(imei) % this.storageGroupCount;
            String group = String.valueOf(l < 10 ? "0" + l : l);

            return deviceId = "root.raw." + group + DOT + imei;
        } catch (NumberFormatException e) {
            return deviceId = "root.raw.999999." + imei;
        }
    }

	private String genDeviceId(String user, String vclId, String tmnlId) {
		String countStr = "50";
		int storageGroupCount = Convert.toInt(countStr);
		// return "root" + DOT + user + DOT + "trans" + DOT + vclId + DOT + tmnlId;
		// storage group name
		return deviceId = "root" + DOT + user + DOT + "trans" + DOT
				+ (padLeft(String.valueOf(Long.parseLong(vclId) % storageGroupCount), countStr.length(), '0')) + DOT
				+ vclId + DOT + tmnlId;
	}



	private static void insert(RowsOfRecords rowsOfRecords)
			throws StatementExecutionException, IoTDBConnectionException {

		insertThreadPool.submit(new SessionInsertThread(rowsOfRecords));

		// sessionPool.testInsertRecords(rowsOfRecords.getDeviceIdList(),
		// rowsOfRecords.getTimestampList(),
		// rowsOfRecords.getMeasurementsList(), rowsOfRecords.getTypeLists(),
		// rowsOfRecords.getValueLists());
	}


}
