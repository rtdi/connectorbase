sap.ui.define([ "jquery.sap.global" ], function(jQuery) {
	return sap.m.MultiInput.extend("com.rtdi.bigdata.connector.ui.components.TransformationStepMultiInput", {
		metadata : {
			properties : {
			},
			aggregations : {
			},
			events : {}
		},
		renderer: {},
		init : function() {
			sap.m.MultiInput.prototype.init.apply(this, arguments);
			var fnValidator = function(args){
				var text = args.text;
				return new sap.m.Token({key: text, text: text});
			};
			var byStepname = function(a, b) {
				if (a.stepname > b.stepname) {
					return 1;
				} else if (a.stepname < b.stepname) {
					return -1;
				} else {
					return 0;
				}
			};
			this.addValidator(fnValidator);
			this.attachTokenUpdate(function(oEvent) {
				var aAddedTokens = oEvent.getParameter("addedTokens");
				var aRemovedTokens = oEvent.getParameter("removedTokens");
				var oData = this.getModel().getData();
				if (!oData.steps) {
					oData.steps = [];
				}
				if (aAddedTokens) {
					for ( var i = 0; i < aAddedTokens.length; i++) {
						oData.steps.push({stepname: aAddedTokens[i].getKey()});
					}
				}
				if (aRemovedTokens) {
					for ( var i = 0; i < aRemovedTokens.length; i++) {
						oData.steps = oData.steps.filter( el => el.stepname !== aRemovedTokens[i].getKey() );
					}
				}
				this.getModel().setProperty("/steps", oData.steps);
				this.getModel().refresh(); // to trigger showing the node and execute the sorter
			});
		},
	});
});
