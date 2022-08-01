package util;

import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.text.SimpleDateFormat;
import java.util.Date;

import static run.Launcher.sessionPool;

public class Util {

	private static SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");

	public static long selectLast(String deviceId) {
		try (SessionDataSetWrapper sessionDataSetWrapper = sessionPool
				.executeQueryStatement("select last work_status_gen_time from " + deviceId)) {

			while (sessionDataSetWrapper.hasNext()) {
				RowRecord next = sessionDataSetWrapper.next();
				Field field = next.getFields().get(1);
				TSDataType dataType = field.getDataType();
				switch (dataType) {
					case TEXT:
						return Long.parseLong(field.getStringValue());
					case INT64:
						return field.getLongV();
					case DOUBLE:
						return (long) field.getDoubleV();
				}
			}

		} catch (Exception e) {
			System.out.print(formatter.format(new Date(System.currentTimeMillis())));
			e.printStackTrace();
		}
		return 0L;
	}
}
