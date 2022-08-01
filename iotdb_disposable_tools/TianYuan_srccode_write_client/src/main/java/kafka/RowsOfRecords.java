package kafka;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

class RowsOfRecords {

	long maxNumber = Config.maxRowNumber;
	long rowNumber = 0;

	boolean organized = false;

	// List<String> deviceIdList = new ArrayList<>();
	String deviceId;
	List<Long> timestampList = new ArrayList<>();
	List<List<String>> measurementsList = new ArrayList<>();
	List<List<TSDataType>> typeLists = new ArrayList<>();
	List<List<Object>> valueLists = new ArrayList<>();

	public RowsOfRecords(String deviceId) {
		this.deviceId = deviceId;
	}

	public boolean shouldInsertAfterAddRecords(long timestamp, List<String> measurements, List<TSDataType> types,
			List<Object> values) {
		if (organized) {
			throw new IllegalArgumentException("Cannot insert to an organized RowsOfRecords");
		}
		// deviceIdList.add(deviceId);
		timestampList.add(timestamp);
		measurementsList.add(measurements);
		typeLists.add(types);
		valueLists.add(values);
		rowNumber++;
		if (rowNumber >= maxNumber) {
			organize();
		}
		return rowNumber >= maxNumber;
	}

	public void reset() {
		rowNumber = 0;
		// deviceIdList = new ArrayList<>();
		timestampList = new ArrayList<>();
		measurementsList = new ArrayList<>();
		typeLists = new ArrayList<>();
		valueLists = new ArrayList<>();
		organized = false;
	}

	public boolean isEmpty() {
		return timestampList.size() == 0;
	}

	// public List<String> getDeviceIdList() {
	// return deviceIdList;
	// }

	public String getDeviceId() {
		return deviceId;
	}

	public List<Long> getTimestampList() {
		return timestampList;
	}

	public List<List<String>> getMeasurementsList() {
		return measurementsList;
	}

	public List<List<TSDataType>> getTypeLists() {
		return typeLists;
	}

	public List<List<Object>> getValueLists() {
		return valueLists;
	}

	public long getRowNumber() {
		return timestampList.size();
	}

	public long getPointNumber() {
		long sum = 0;
		for (List<Object> o : valueLists) {
			sum += o.size();
		}
		return sum;
	}

	public void organize() {
		if (!organized) {

			Integer[] order = new Integer[timestampList.size()];
			for (int i = 0; i < order.length; i++) {
				order[i] = i;
			}

			Arrays.sort(order, Comparator.comparingLong(o -> timestampList.get(o)));

			timestampList.sort(Long::compareTo);

			long lastTimestamp = -1;
			List<Long> newTimestamps = new ArrayList<>();
			List<List<String>> newMeasurementsList = new ArrayList<>();
			List<List<TSDataType>> newTypeLists = new ArrayList<>();
			List<List<Object>> newValueLists = new ArrayList<>();

			List<String> currentMeasurementList = new ArrayList<>();
			List<TSDataType> currentTypeList = new ArrayList<>();
			List<Object> currentValueList = new ArrayList<>();
			for (int i = 0; i < order.length; i++) {
				long currentTimestamp = timestampList.get(i);
				if (lastTimestamp == -1) {
					lastTimestamp = currentTimestamp;
					newTimestamps.add(currentTimestamp);
					currentMeasurementList.addAll(measurementsList.get(order[i]));
					currentTypeList.addAll(typeLists.get(order[i]));
					currentValueList.addAll(valueLists.get(order[i]));
				} else if (lastTimestamp == currentTimestamp) {
					currentMeasurementList.addAll(measurementsList.get(order[i]));
					currentTypeList.addAll(typeLists.get(order[i]));
					currentValueList.addAll(valueLists.get(order[i]));
				} else {
					lastTimestamp = currentTimestamp;
					newTimestamps.add(currentTimestamp);
					newMeasurementsList.add(currentMeasurementList);
					newTypeLists.add(currentTypeList);
					newValueLists.add(currentValueList);
					currentMeasurementList = new ArrayList<>();
					currentTypeList = new ArrayList<>();
					currentValueList = new ArrayList<>();
					currentMeasurementList.addAll(measurementsList.get(order[i]));
					currentTypeList.addAll(typeLists.get(order[i]));
					currentValueList.addAll(valueLists.get(order[i]));
				}
			}
			if (!currentMeasurementList.isEmpty()) {
				newMeasurementsList.add(currentMeasurementList);
				newTypeLists.add(currentTypeList);
				newValueLists.add(currentValueList);
			}

			assert newMeasurementsList.size() == newTimestamps.size();
			assert newTypeLists.size() == newTimestamps.size();
			assert newValueLists.size() == newTimestamps.size();

			// deviceIdList = deviceIdList.subList(0, newTimestamps.size());

			timestampList = newTimestamps;
			measurementsList = newMeasurementsList;
			typeLists = newTypeLists;
			valueLists = newValueLists;

			organized = true;

		}
	}
}