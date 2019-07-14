package io.rtdi.bigdata.connector.pipeline.foundation.mapping;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;

import io.rtdi.bigdata.connector.pipeline.foundation.mapping.RecordMapping.RecordContext;

public abstract class Mapping {

	protected static final JexlEngine jexl = new JexlBuilder().cache(512).strict(true).silent(false).create();
	protected JexlExpression expression;
	
	protected Mapping(JexlExpression e) {
		this.expression = e;
	}

	protected Mapping(String e) {
		this(jexl.createExpression(e));
	}

	public Object evaluate(RecordContext context) {
		Object o = expression.evaluate(context);
		return o;
	}

}
