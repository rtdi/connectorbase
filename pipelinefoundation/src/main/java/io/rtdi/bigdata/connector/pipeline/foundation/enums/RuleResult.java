package io.rtdi.bigdata.connector.pipeline.foundation.enums;

public enum RuleResult {
	PASS,
	WARN,
	FAIL;

	public float getDefaultQuality() {
		switch (this) {
		case PASS: return 1.0f;
		case FAIL: return 0.0f;
		default: return 0.9f;
		}
	}
	
	public RuleResult aggregate(RuleResult ruleresult) {
		if (ruleresult == null) {
			return null;
		} else if (this == FAIL || ruleresult == FAIL) {
			return FAIL;
		} else if (this == WARN || ruleresult == WARN) {
			return WARN;
		} else {
			return PASS;
		}
	}
}
