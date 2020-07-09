sap.ui.define([ "jquery.sap.global", "com/rtdi/bigdata/connector/ui/components/ErrorMessageItem" ], function(jQuery) {
	return sap.ui.core.Control.extend("com.rtdi.bigdata.connector.ui.components.ErrorMessageButton", {
		metadata : {
			properties : {
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
		 		type: sap.m.ButtonType.Default,
		 		text: "0",
		 		press: this.onPopoverPress
		 	});
		 	this.setAggregation("_btn", oBtn );
			var oPopover = new sap.m.MessagePopover();
		 	this.setAggregation("_popover", oPopover, true);	
		},
		_getPopover : function() {
			return this.getAggregation("_popover");
		},
		addItem : function(oItem) {
			this.addAggregation("items", oItem, true);
			this._setMessagecount(this.getAggregation("items").length);
		},
		removeItem : function(oItem) {
			this.removeAggregation("items", oItem, true);
			this._setMessagecount(this.getAggregation("items").length);
		},
		removeItems : function() {
			this.removeAllAggregation("items", true);
			this._setMessagecount(0);
		},
		_setMessagecount : function(value) {
			var oBtn = this.getAggregation("_btn");
			oBtn.setText(value);
			if (value === 0) {
				oBtn.setType(sap.m.ButtonType.Default);
			} else {
				oBtn.setType(sap.m.ButtonType.Emphasized);
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
