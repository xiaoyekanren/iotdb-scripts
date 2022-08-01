package kafka;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class BlockName {

	public static final Set<String> LOG_WORKSTATUS_NAMES = new HashSet<>(
			Arrays.asList("TY_0002_00_160", "TY_0002_00_161", "TY_0002_00_162", "TY_0002_00_163", "TY_0002_00_164",
					"TY_0002_00_165", "TY_0002_00_166", "TY_0002_00_167", "TY_0002_00_168", "TY_0002_00_169",
					"TY_0002_00_170", "TY_0002_00_171", "TY_0002_00_172", "TY_0002_00_173", "TY_0002_00_731",
					"TY_0002_00_1185", "TY_0002_00_732_gather", "TY_0002_00_732_gather", "TY_0002_00_733_gather"));
	public static final Set<String> FAULT_HIST_WORKSTATUS_NAMES = new HashSet<>(Arrays.asList("TY_0002_00_8"));
	public static final Set<String> ALARM_HIST_WORKSTATUS_NAMES = new HashSet<>(Arrays.asList("TY_0002_00_52",
			"TY_0002_00_53", "TY_0002_00_54", "TY_0002_00_55", "TY_0002_00_56", "TY_0002_00_57", "TY_0002_00_58",
			"TY_0002_00_59", "TY_0002_00_60", "TY_0002_00_61", "TY_0002_00_62", "TY_0002_00_63", "TY_0002_00_64",
			"TY_0002_00_65", "TY_0002_00_66", "TY_0002_00_67", "TY_0002_00_68", "TY_0002_00_69", "TY_0002_00_1180",
			"TY_0002_00_1182", "TY_0002_00_1184", "TY_0002_00_1192"));
	// "TY_0002_00_4" and "TY_0002_00_5" are treated as normal work status data
	public static final Set<String> POSITION_WORKSTATUS_NAMES = new HashSet<>(Arrays.asList("TC_0001_00_1",
			"TY_0002_00_4_Geo_State", "TY_0002_00_4_GeoAdt_State", "TY_0002_00_4_GeoAltitude_State"));
	public static final Set<String> EVENT_WORKSTATUS_NAMES = new HashSet<>(Arrays.asList("TY_0002_00_678_gather",
			"TY_0002_00_681_gather", "TY_0002_00_686_gather", "TY_0002_00_688_gather", "TY_0002_00_691_gather",
			"TY_0002_00_693_gather", "TY_0002_00_698_gather", "TY_0002_00_702_gather", "TY_0002_00_705_gather",
			"TY_0002_00_710_gather", "TY_0002_00_713_gather", "TY_0002_00_716_gather", "TY_0002_00_720_gather",
			"TY_0002_00_725_gather", "TY_0002_00_1010_gather"));
	public static final Set<String> FAULT_MATCH_WORKSTATUS_NAME = new HashSet<>(Arrays.asList("TY_0002_00_8_Fault"));
	public static final Set<String> ALARM_MATCH_WORKSTATUS_NEW = new HashSet<>(Arrays.asList("TY_0001_00_45_Alarm"));
}
