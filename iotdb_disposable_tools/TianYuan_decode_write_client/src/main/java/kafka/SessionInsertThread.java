package kafka;

import static run.Launcher.sessionPool;

import java.awt.Transparency;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import ty.pub.TransPacket;

public class SessionInsertThread implements Runnable {

  private List<String> deviceIds;
  private String deviceId = "";
  private List<Long> times;
  private List<List<String>> measurementsList;
  private List<List<TSDataType>> typeList;
  private List<List<Object>> valuesList;

  SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");

//  TransPacket transPacket;
  long rowNumber;
  long pointNumber;

//  public SessionInsertThread(
//      List<String> deviceIds,
//      List<Long> times,
//      List<List<String>> measurementsList,
//      List<List<TSDataType>> typeList,
//      List<List<Object>> valuesList) {
//    this.deviceIds = deviceIds;
//    this.times = times;
//    this.measurementsList = measurementsList;
//    this.typeList = typeList;
//    this.valuesList = valuesList;
//  }

  public SessionInsertThread(RowsOfRecords rowsOfRecords) {
//    this.deviceIds = rowsOfRecords.getDeviceIdList();
    this.deviceId = rowsOfRecords.getDeviceId();
    this.times = rowsOfRecords.getTimestampList();
    this.measurementsList = rowsOfRecords.getMeasurementsList();
    this.typeList = rowsOfRecords.getTypeLists();
    this.valuesList = rowsOfRecords.getValueLists();
    this.rowNumber = rowsOfRecords.getRowNumber();
    this.pointNumber = rowsOfRecords.getPointNumber();

//    this.deviceIds = new ArrayList<>();
//    for(int i = 0; i < times.size(); i++){
//      deviceIds.add(deviceId);
//    }
//    this.transPacket = transPacket;
  }

//  public SessionInsertThread(RowsOfRecords rowsOfRecords) {
//    this.deviceIds = rowsOfRecords.getDeviceIdList();
//    this.times = rowsOfRecords.getTimestampList();
//    this.measurementsList = rowsOfRecords.getMeasurementsList();
//    this.typeList = rowsOfRecords.getTypeLists();
//    this.valuesList = rowsOfRecords.getValueLists();
//  }

  @Override
  public void run() {
    try {

//      Date date = new Date();
//      System.out.println("device: " + deviceIds.size() + "time: " + times.size());
//      long start = System.currentTimeMillis();
//      sessionPool.testInsertRecords(deviceIds, times, measurementsList, typeList, valuesList);
//      long end = System.currentTimeMillis();
//      long testDuration = end - start;
//      System.out.println("test insert, " + (System.currentTimeMillis() - start));
//      start = System.currentTimeMillis();
      sessionPool.insertOneDeviceRecords(deviceId, times, measurementsList, typeList, valuesList);
//      List<String> deviceIds = new ArrayList<>();
//      for(int i = 0; i < times.size(); i++){
//        deviceIds.add(deviceId);
//      }
//      System.out.println(">>>>>>>>>>>");
//      sessionPool.testInsertRecords(deviceIds, times, measurementsList, typeList, valuesList);
//      end = System.currentTimeMillis();
//      long insertDuration = end - start;
//      System.out.println("test duration, " + testDuration + ", insert duration, " + insertDuration
//          + ", row number, " + rowNumber + ", point number, " + pointNumber);

//      switch (Config.USER_NAME) {
//        case "CTY":
//          if (testDuration > 800) {
//            ObjectWriter
//                .write(transPacket, deviceIds, times, measurementsList, typeList, valuesList);
//          }
//          break;
//        case "Kobelco":
//          if (testDuration > 25000) {
//            ObjectWriter
//                .write(transPacket, deviceIds, times, measurementsList, typeList, valuesList);
//          }
//          break;
//        default:
//          break;
//      }
      /*if (end - start > 10000) {
        System.out.println("@<<" + date + " " + Thread.currentThread().getName() + " " + deviceIds.get(0) + " " + times.get(0));
        System.out.println("@<<" + new Date() + " " + Thread.currentThread().getName() + " " + deviceIds.get(0) + " " + times.get(0) + "  " + (end - start) + " OK");
      }*/
    } catch (Exception e) {
      System.out.print(formatter.format(new Date(System.currentTimeMillis())));
      e.printStackTrace();
    }
  }
}
