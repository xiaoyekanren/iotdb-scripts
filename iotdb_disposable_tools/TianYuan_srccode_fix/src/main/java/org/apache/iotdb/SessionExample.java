/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionExample.class);
  private static final Logger STATISTICS_LOGGER = LoggerFactory.getLogger("STATISTICS");

  private static final File sgSkipLog = new File("./logs/sg-skip.log");
  private static final File tsSkipLog = new File("./logs/ts-skip.log");
  private static BufferedWriter sgBufferedWriter;
  private static BufferedWriter tsBufferedWriter;

  private static final String TY_HOST = "192.168.35.21";
  private static final String TY_BACKUP_HOST = "192.168.35.21";
  private static final String LOCAL_HOST = "127.0.0.1";
  private static final String RPC_PORT = "6668";
  private static final String USERNAME = "root";
  private static final String PASSWORD = "root";

  private static final int N_THREADS = 300;
  private static final long TIMEOUT = 600000L;

  private static SessionPool pool;
  private static SessionPool pool_backup;

  private static HashMap<String, List<String>> skipTsMap = new HashMap<>();

  public static void main(String[] args) throws IOException {
    pool =
        new SessionPool(
            TY_HOST,
            Integer.parseInt(RPC_PORT),
            USERNAME,
            PASSWORD,
            4 * N_THREADS,
            5000,
            TIMEOUT,
            false,
            (ZoneId) null,
            false);

    pool_backup =
        new SessionPool(
            TY_BACKUP_HOST,
            Integer.parseInt(RPC_PORT),
            USERNAME,
            PASSWORD,
            4 * N_THREADS,
            5000,
            TIMEOUT,
            false,
            (ZoneId) null,
            false);

    List<String> sgList = new ArrayList<>();
    List<String> skipSgList = getSkipSgList(); // 跳过已处理存储组
    skipTsMap = getSkipTsMap(); // 跳过已处理的时间序列

    SessionDataSetWrapper sgSet = null;
    try {
      // 查看所有存储组
      sgSet = pool_backup.executeQueryStatement("show storage group root.raw.*");

      while (sgSet.hasNext()) {
        RowRecord sgRowRecord = sgSet.next();
        String sgName = sgRowRecord.getFields().get(0).toString();
        if (!skipSgList.contains(sgName)) {
          sgList.add(sgName);
        } else {
          LOGGER.info("Skip the storage group: " + sgName);
        }
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      LOGGER.error("Exception when executing sql show storage group root.raw.*", e);
    } finally {
      pool_backup.closeResultSet(sgSet);
    }

    sgBufferedWriter = new BufferedWriter(new FileWriter(sgSkipLog, true));
    tsBufferedWriter = new BufferedWriter(new FileWriter(tsSkipLog, true));

    for (String sgName : sgList) {
      executeForEachSG(sgName);
    }

    sgBufferedWriter.flush();
    tsBufferedWriter.flush();
    sgBufferedWriter.close();
    tsBufferedWriter.close();

    pool.close();
    pool_backup.close();
  }

  private static HashMap<String, List<String>> getSkipTsMap() throws IOException {
    HashMap<String, List<String>> skipTsMap = new HashMap<>();
    if (!tsSkipLog.exists()) return skipTsMap;
    BufferedReader tsBufferedReader = new BufferedReader(new FileReader(SessionExample.tsSkipLog));
    String line = "";
    String[] arr = null;
    while ((line = tsBufferedReader.readLine()) != null) {
      arr = line.split(",");
      List<String> list = skipTsMap.computeIfAbsent(arr[0], n -> new ArrayList<>());
      list.add(arr[1]);
    }
    tsBufferedReader.close();
    return skipTsMap;
  }

  private static List<String> getSkipSgList() throws IOException {
    List<String> skipSgList = new ArrayList<>();
    if (!sgSkipLog.exists()) return skipSgList;
    BufferedReader sgBufferedReader = new BufferedReader(new FileReader(SessionExample.sgSkipLog));
    String line = "";
    while ((line = sgBufferedReader.readLine()) != null) {
      skipSgList.add(line);
    }
    sgBufferedReader.close();
    return skipSgList;
  }

  private static void putRawJsonToKafka(String rawJson) {
    // put this rawJson to kafka
  }

  // 处理一个存储组
  public static <pair> void executeForEachSG(String sgName) throws IOException {

    // 查看所有源码序列
    List<String> timeSeriesList = new ArrayList<>();
    SessionDataSetWrapper timeSeriesSet = null;

    try {
      timeSeriesSet =
          pool_backup.executeQueryStatement("show timeseries " + sgName + ".*.TY_0001_Raw_Packet");
      while (timeSeriesSet.hasNext()) {
        RowRecord tsRowRecord = timeSeriesSet.next();
        String timeseries = tsRowRecord.getFields().get(0).toString();
        if (!skipTsMap.containsKey(sgName) || !skipTsMap.get(sgName).contains(timeseries)) {
          timeSeriesList.add(timeseries);
        } else {
          LOGGER.info("Skip the timeseries: " + timeseries);
        }
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      LOGGER.error(
          "Exception when executing sql show timeseries " + sgName + ".*.TY_0001_Raw_Packet", e);
    } finally {
      pool_backup.closeResultSet(timeSeriesSet);
    }

    ExecutorService fixedThreadPool = Executors.newFixedThreadPool(N_THREADS);

    LOGGER.info("Start checking the storage group: " + sgName);

    AtomicBoolean errorFlag = new AtomicBoolean(false);
    for (String timeseries : timeSeriesList) {
      fixedThreadPool.submit(
          () -> {
            String deviceId = getDeviceId(timeseries);
            String sql = getQuerySql(timeseries);
            boolean tsErrorFlag = false;
            HashMap<String, Pair<Integer, Integer>> statistics = new HashMap<>();

            LOGGER.info(
                "Start checking the timeseries: "
                    + timeseries
                    + " ( deviceId = "
                    + deviceId
                    + " )");

            SessionDataSetWrapper dataSet = null;

            try {
              dataSet = pool_backup.executeQueryStatement(sql);

              boolean isMatched = false;

              String curIMEI = null;
              List<Long> timeList = new ArrayList<>();
              List<String> jsonList = new ArrayList<>();

              while (true) {

                if (!dataSet.hasNext()) {
                  if (timeList.size() != 0) { // 若结尾一段是错误数据，进行检查
                    checkIfExistMayInsert(timeseries, curIMEI, timeList, jsonList);
                  }
                  break;
                }

                RowRecord dataRowRecord = dataSet.next();
                long time = dataRowRecord.getTimestamp();
                String rawJson = dataRowRecord.getFields().get(0).toString();
                String iMEI = getiMEI(rawJson);
                Pair<Integer, Integer> statisticsPair =
                    statistics.computeIfAbsent(
                        new SimpleDateFormat("yyyy-MM-dd").format(time), n -> new Pair<>(0, 0));

                if (!iMEI.equals(deviceId)) {
                  statisticsPair.left++;
                  if (!isMatched) { // （正确数据后）出现错误数据：isMatched标记为true，并记录iMEI
                    curIMEI = iMEI;
                    isMatched = true;
                  } else { // （错误数据后）继续出现错误数据
                    if (!iMEI.equals(curIMEI)) { // 如果本次错误数据与之前错误数据设备不一样，对之前的错误数据区间进行检查，然后继续记录当前区间
                      checkIfExistMayInsert(timeseries, curIMEI, timeList, jsonList);
                      timeList.clear();
                      jsonList.clear();
                      curIMEI = iMEI;
                    }
                  }
                  timeList.add(time);
                  jsonList.add(rawJson);
                  continue;
                } else {
                  statisticsPair.right++;
                  if (isMatched) { // （错误数据后）出现正确数据，说明之前一段区间是错误数据，对该区间进行检查
                    checkIfExistMayInsert(timeseries, curIMEI, timeList, jsonList);
                    isMatched = false;
                    timeList.clear();
                    jsonList.clear();
                  }
                }

                // put this rawJson to kafka [1]
                putRawJsonToKafka(rawJson);
              }

            } catch (StatementExecutionException | IoTDBConnectionException e) {
              errorFlag.set(true);
              tsErrorFlag = true;
              LOGGER.error(
                  "Exception when checking storage group: "
                      + sgName
                      + ", timeseries: "
                      + timeseries,
                  e);
            } finally {
              if (!tsErrorFlag) {
                LOGGER.info("The timeseries is checked successfully: " + timeseries);
                for (Map.Entry<String, Pair<Integer, Integer>> entry : statistics.entrySet()) {
                  String date = entry.getKey();
                  int misMatchedCount = entry.getValue().left;
                  int matchedCount = entry.getValue().right;
                  STATISTICS_LOGGER.info(
                      "timeseries: {}, deviceId: {}, date: {}, totalCount: {}, matchedCount: {}, misMatchedCount: {}",
                      timeseries,
                      deviceId,
                      date,
                      matchedCount + misMatchedCount,
                      matchedCount,
                      misMatchedCount);
                }
                try {
                  tsBufferedWriter.write(sgName + "," + timeseries + "\n");
                  tsBufferedWriter.flush();
                } catch (IOException e) {
                  e.printStackTrace();
                }
              } else {
                LOGGER.info("The timeseries is checked failed: " + timeseries);
              }
              pool_backup.closeResultSet(dataSet);
            }
          });
    }

    fixedThreadPool.shutdown();

    while (true) {
      if (fixedThreadPool.isTerminated()) break;
    }

    if (errorFlag.get()) {
      LOGGER.info("The storage group is checked failed: " + sgName);
    } else {
      LOGGER.info("The storage group is checked successfully: " + sgName);
      sgBufferedWriter.write(sgName + "\n");
      sgBufferedWriter.flush();
      clearTsSkipFile();
    }
  }

  // 清空当前ts-skip.log
  private static void clearTsSkipFile() throws IOException {
    FileWriter fileWriter = new FileWriter(tsSkipLog, false);
    fileWriter.write("");
    fileWriter.flush();
    fileWriter.close();
  }

  /* 检查数据是否存在，如果存在则跳过，不存在则写入并发至 kafka */
  private static void checkIfExistMayInsert(
      String timeseries, String curIMEI, List<Long> timeList, List<String> jsonList)
      throws IoTDBConnectionException, StatementExecutionException {

    long startTime = timeList.get(0);
    long endTime = timeList.get(timeList.size() - 1);
    String newPath = getNewPath(timeseries, curIMEI);

    LOGGER.info(
        "Mismatched raw packet in timeseries: "
            + timeseries
            + ", time: ["
            + startTime
            + ", "
            + endTime
            + "] "
            + "( Check correct timeseries: "
            + newPath
            + ".TY_0001_Raw_Packet )");

    List<Long> curTimeList = new ArrayList<>();
    List<String> curJsonList = new ArrayList<>();
    SessionDataSetWrapper wrapper = null;
    String querySql =
        "select TY_0001_Raw_Packet from "
            + newPath
            + " where time >= "
            + startTime
            + " and time <= "
            + endTime;
    try {
      wrapper = pool_backup.executeQueryStatement(querySql);

      while (wrapper.hasNext()) {
        RowRecord dataRowRecord = wrapper.next();
        long curTime = dataRowRecord.getTimestamp();
        String curJson = dataRowRecord.getFields().get(0).toString();
        curTimeList.add(curTime);
        curJsonList.add(curJson);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      LOGGER.error("Exception when executing sql " + querySql, e);
    } finally {
      pool_backup.closeResultSet(wrapper);
    }

    // List<MeasurementSchema> schemaList = new ArrayList<>();
    // schemaList.add(new MeasurementSchema("TY_0001_Raw_Packet", TSDataType.TEXT));
    // Tablet tablet = new Tablet(newPath, schemaList);

    // 检查错误数据是否存在相应的正确数据，如果不存在则重新插入
    for (int i = 0; i < timeList.size(); i++) {
      boolean isExist = false;
      for (int j = 0; j < curTimeList.size(); j++) {
        if (timeList.get(i).equals(curTimeList.get(j))
            && getiMEI(jsonList.get(i)).equals(getiMEI(curJsonList.get(j)))) {
          isExist = true;
          break;
        }
      }
      if (!isExist) {
        LOGGER.trace(
            "Mismatched raw packet in timeseries: "
                + timeseries
                + ", time: "
                + timeList.get(i)
                + ", rawJson: "
                + jsonList.get(i)
                + " ( Non-exist in "
                + newPath
                + " )");

        // 插入数据
        // int rowIndex = tablet.rowSize++;
        // tablet.addTimestamp(rowIndex, timeList.get(i));
        // tablet.addValue("TY_0001_Raw_Packet", rowIndex, jsonList.get(i));
        // if (tablet.rowSize == tablet.getMaxRowNumber()) {
        //   insertTablet(newPath, tablet);
        //   tablet.reset();
        // }

        insertRecord(newPath, timeList.get(i), jsonList.get(i));
      } else {
        LOGGER.trace(
            "Mismatched raw packet in timeseries: "
                + timeseries
                + ", time: "
                + timeList.get(i)
                + ", rawJson: "
                + jsonList.get(i)
                + " ( Exist in "
                + newPath
                + " )");
      }

      // put this rawJson to kafka [2]
      putRawJsonToKafka(jsonList.get(i));
    }

    // 插入数据
    // if (tablet.rowSize != 0) {
    //   insertTablet(newPath, tablet);
    //   tablet.reset();
    // }

    // 删除错误数据
    deleteData(timeseries, startTime, endTime);
  }

  // 插入数据（单点）
  private static void insertRecord(String newPath, long time, String rawJson) {
    List<String> measurements = new ArrayList<>();
    measurements.add("TY_0001_Raw_Packet");
    List<String> values = new ArrayList<>();
    values.add(rawJson);

    boolean errorFlag = false;
    try {
      pool.insertRecord(newPath, time, measurements, values);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      errorFlag = true;
      LOGGER.error(
          "Exception when insert timeseries: " + newPath + ".TY_0001_Raw_Packet, time: " + time, e);
    }

    if (!errorFlag) {
      LOGGER.info("Insert timeseries: " + newPath + ".TY_0001_Raw_Packet, time: " + time);
      LOGGER.trace(
          "Insert timeseries: "
              + newPath
              + ".TY_0001_Raw_Packet, time: "
              + time
              + ", rawJson: "
              + rawJson);
    }
  }

  // 插入数据
  private static void insertTablet(String newPath, Tablet tablet) {
    boolean errorFlag = false;
    try {
      pool.insertTablet(tablet);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      errorFlag = true;
      LOGGER.error("Exception when insert timeseries: " + newPath + ".TY_0001_Raw_Packet", e);
    }

    if (!errorFlag) {
      LOGGER.info(
          "Insert timeseries: " + newPath + ".TY_0001_Raw_Packet, data count: " + tablet.rowSize);
      LOGGER.trace(
          "Insert timeseries: " + newPath + ".TY_0001_Raw_Packet, data count: " + tablet.rowSize);
    }
  }

  // 删除数据
  private static void deleteData(String timeseries, long startTime, long endTime) {
    List<String> paths = new ArrayList<>();
    paths.add(timeseries);

    // 删除数据
    boolean errorFlag = false;
    try {
      pool.deleteData(paths, startTime, endTime);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      errorFlag = true;
      LOGGER.error(
          "Exception when delete timeseries: "
              + timeseries
              + ", time: ["
              + startTime
              + ", "
              + endTime
              + "]",
          e);
    }

    if (!errorFlag) {
      LOGGER.info(
          "Delete timeseries: " + timeseries + ", time: [" + startTime + ", " + endTime + "]");
      LOGGER.trace(
          "Delete timeseries: " + timeseries + ", time: [" + startTime + ", " + endTime + "]");
    }
  }

  private static String getNewPath(String timeseries, String iMEI) {
    String[] splits = timeseries.split("[.]");
    splits[splits.length - 2] = "8000" + iMEI;

    StringBuilder newPath = new StringBuilder();
    for (int i = 0; i < splits.length - 1; i++) {
      newPath.append(splits[i]);
      if (i != splits.length - 2) newPath.append('.');
    }
    return newPath.toString();
  }

  private static String getDeviceId(String timeseries) {
    String[] splits = timeseries.split("[.]");
    return splits[3].substring(4);
  }

  private static String getQuerySql(String timeseries) {
    String[] splits = timeseries.split("[.]");
    String path =
        timeseries.substring(0, timeseries.length() - splits[splits.length - 1].length() - 1);
    return "select TY_0001_Raw_Packet from " + path + " where time < 2021-08-08T00:00:00.000+08:00";
  }

  private static String getiMEI(String jsonStr) {
    String[] iMEIs = jsonStr.split("[\"]");
    return iMEIs[3];
  }
}
