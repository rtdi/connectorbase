sap.ui.define([ "jquery.sap.global" ], function(jQuery) {
	return sap.ui.core.Control.extend("PropertiesInput", {
		metadata : {
			properties : {
				"type" : "string",
				"value" : "string",
				"enabled": "boolean"
			},
			aggregations : {
				"_control" : {
					type : "sap.ui.core.Control",
					multiple : false,
					visibility : "hidden"
				},
				"suggestionItems" : {
					type : "sap.ui.core.Item",
					multiple : true
				},
			},
			events : {}
		},
		init : function() {
		},
		setValue : function(value) {
			this.setProperty("value", value, true);
			if (this.getAggregation("_control")) {
				this.getAggregation("_control").setValue(value);
			}
		},
		setType : function(type) {
			var that = this;
			this.setAggregation("_control", undefined);
			if (type === "PropertyString") {
				this.setAggregation("_control", new sap.m.Input({
					type : sap.m.InputType.Text,
					change: function(oEvent){
						that.setProperty("value", oEvent.getParameter("newValue"), true);
					}
				}));
			} else if (type === "PropertyText") {
				this.setAggregation("_control", new sap.m.TextArea( { 
					width: "100%",
					change: function(oEvent){
						that.setProperty("value", oEvent.getParameter("newValue"), true);
					}
				} ));
			} else if (type === "PropertyInt" || type === "PropertyLong") {
				this.setAggregation("_control", new sap.m.Input({
					type : sap.m.InputType.Number,
					change: function(oEvent){
						that.setProperty("value", oEvent.getParameter("newValue"), true);
					}
				}));
			} else if (type === "PropertyPassword") {
				this.setAggregation("_control", new sap.m.Input({
					type : sap.m.InputType.Password,
					change: function(oEvent){
						that.setProperty("value", oEvent.getParameter("newValue"), true);
					}
				}));
			} else if (type === "PropertySchemaSelector") {
				var oControl = new sap.m.ComboBox( {
					items: {path: 'schemas>/tablenames',
							sorter: { path: 'tablename'},
							template: new sap.ui.core.Item( { key: '{schemas>tablename}', text: '{schemas>tablename}' })
					},
					width: "100%",
					change: function(oEvent){
						that.setProperty("value", oEvent.getParameter("newValue"), true);
					}
				});
				this.setAggregation("_control", oControl);
			} else if (type === "PropertyTopicSelector") {
				var oControl = new sap.m.ComboBox( {
					items: {path: 'topics>/topics',
							sorter: { path: 'topicname'},
							template: new sap.ui.core.Item( { key: '{topics>topicname}', text: '{topics>topicname}' })
					},
					width: "100%",
					change: function(oEvent){
						that.setProperty("value", oEvent.getParameter("newValue"), true);
					}
				});
				this.setAggregation("_control", oControl);
			} else if (type === "PropertyMultiSchemaSelector") {
				var oControl = new sap.m.MultiComboBox( {
					items: {path: 'schemas>/tablenames',
						sorter: { path: 'tablename'},
						template: new sap.ui.core.Item( { key: '{schemas>tablename}', text: '{schemas>tablename}' })
					},
					width: "100%",
					selectionChange: function(oEvent){
						that.setProperty("value", oEvent.getParameter("newValue"), true);
					}
				});
				this.setAggregation("_control", oControl);
			}
			if (this.getAggregation("_control")) {
				this.getAggregation("_control").setValue(
						this.getProperty("value"));
				this.getAggregation("_control").setEnabled(
						this.getProperty("enabled"));
			}
		},
		setEnabled : function(value) {
			this.setProperty("enabled", value);
			if (this.getAggregation("_control")) {
				this.getAggregation("_control").setEnabled(value);
			}
		},
		getValue : function() {
			if (this.getAggregation("_control")) {
				return this.getAggregation("_control").getValue(value);
			}
		},
		renderer : function(oRenderManager, oControl) {
			oRenderManager.write("<div");
			oRenderManager.writeControlData(oControl);
			oRenderManager.writeClasses();
			oRenderManager.write(">");
			oRenderManager.renderControl(oControl.getAggregation("_control"));
			oRenderManager.write("</div>");
		}
	});
});
