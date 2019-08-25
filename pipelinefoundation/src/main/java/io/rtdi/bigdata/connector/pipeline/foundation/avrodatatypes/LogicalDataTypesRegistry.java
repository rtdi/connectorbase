package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalTypes;

public class LogicalDataTypesRegistry {
	private static boolean registered = false;
	
	public static void registerAll() {
		if (!registered) {
			LogicalTypes.register(AvroBoolean.NAME, AvroBoolean.factory);
			LogicalTypes.register(AvroByte.NAME, AvroByte.factory);
			LogicalTypes.register(AvroBytes.NAME, AvroBytes.factory);
			LogicalTypes.register(AvroCLOB.NAME, AvroCLOB.factory);
			LogicalTypes.register(AvroDate.NAME, AvroDate.factory);
			LogicalTypes.register(AvroDecimal.NAME, AvroDecimal.factory);
			LogicalTypes.register(AvroDouble.NAME, AvroDouble.factory);
			LogicalTypes.register(AvroEnum.NAME, AvroEnum.factory);
			LogicalTypes.register(AvroFixed.NAME, AvroFixed.factory);
			LogicalTypes.register(AvroFloat.NAME, AvroFloat.factory);
			LogicalTypes.register(AvroInt.NAME, AvroInt.factory);
			LogicalTypes.register(AvroLong.NAME, AvroLong.factory);
			LogicalTypes.register(AvroMap.NAME, AvroMap.factory);
			LogicalTypes.register(AvroNCLOB.NAME, AvroNCLOB.factory);
			LogicalTypes.register(AvroNVarchar.NAME, AvroNVarchar.factory);
			LogicalTypes.register(AvroShort.NAME, AvroShort.factory);
			LogicalTypes.register(AvroSTGeometry.NAME, AvroSTGeometry.factory);
			LogicalTypes.register(AvroSTPoint.NAME, AvroSTPoint.factory);
			LogicalTypes.register(AvroString.NAME, AvroString.factory);
			LogicalTypes.register(AvroTime.NAME, AvroTime.factory);
			LogicalTypes.register(AvroUri.NAME, AvroUri.factory);
			LogicalTypes.register(AvroUUID.NAME, AvroUUID.factory);
			LogicalTypes.register(AvroVarchar.NAME, AvroVarchar.factory);
			registered = true;
		}
	}

}
