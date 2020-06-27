sap.ui.define([ "jquery.sap.global", "com/rtdi/bigdata/connector/ui/components/ErrorMessageItem" ], function(jQuery) {
	return sap.ui.core.Control.extend("com.rtdi.bigdata.connector.ui.components.ErrorMessageButton", {
		metadata : {
			properties : {
				messagecount: "int"
			},
			aggregations : {
				_btn : {
					type : "sap.m.Button",
					multiple : false,
					visibility : "hidden"
				},
				_popover : {
					type : "sap.m.MessagePopover",
					multiple : false,
					visibility : "hidden"
				},
				defaultAggregation: "items",
				items : {
					type : "com.rtdi.bigdata.connector.ui.components.ErrorMessageItem",
					multiple : true,
					singularName : "item",
					forwarding: {
						getter: "_getPopover",
						aggregation: "items"
					}
				}
			},
			events : {}
		},
		init : function() {
			var oBtn = new sap.m.Button( {
		 		icon: "sap-icon://alert",
		 		type: sap.m.ButtonType.Emphasized,
		 		press: this.onPopoverPress
		 	});
		 	this.setAggregation("_btn", oBtn );
			var oPopover = new sap.m.MessagePopover();
		 	this.setAggregation("_popover", oPopover, true);		 	
		},
		_getPopover : function() {
			return this.getAggregation("_popover");
		},
		setMessagecount : function(value) {
			this.setProperty("messagecount", value);
			var oBtn = this.getAggregation("_btn");
			oBtn.setText(value);
			if (value === 0) {
				oBtn.setVisible(false);
			} else {
				oBtn.setVisible(true);
			}
		},
		onPopoverPress : function(oEvent) {
			// this = is the _btn instance 
			var oErrorButton = this.getParent();
			var oPopover = oErrorButton._getPopover();
			oPopover.openBy(oEvent.getSource());
		},
		renderer : function(oRenderManager, oControl) {
			oRenderManager.write("<div");
			oRenderManager.writeControlData(oControl);
			oRenderManager.writeClasses();
			oRenderManager.write(">");
			oRenderManager.renderControl(oControl.getAggregation("_btn"));
			oRenderManager.write("</div>");
		}
	});
});
