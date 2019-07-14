package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalTypes;

public class LogicalDataTypesRegistry {
	private static boolean registered = false;
	
	public static void registerAll() {
		if (!registered) {
			LogicalTypes.register(AvroByte.NAME, AvroByte.factory);
			LogicalTypes.register(AvroCLOB.NAME, AvroCLOB.factory);
			LogicalTypes.register(AvroNCLOB.NAME, AvroNCLOB.factory);
			LogicalTypes.register(AvroNVarchar.NAME, AvroNVarchar.factory);
			LogicalTypes.register(AvroShort.NAME, AvroShort.factory);
			LogicalTypes.register(AvroSTGeometry.NAME, AvroSTGeometry.factory);
			LogicalTypes.register(AvroSTPoint.NAME, AvroSTPoint.factory);
			LogicalTypes.register(AvroUri.NAME, AvroUri.factory);
			LogicalTypes.register(AvroVarchar.NAME, AvroVarchar.factory);
			registered = true;
		}
	}

}
