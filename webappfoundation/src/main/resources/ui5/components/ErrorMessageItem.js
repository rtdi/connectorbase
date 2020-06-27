sap.ui.define([ "jquery.sap.global" ], function(jQuery) {
	return sap.m.MessageItem.extend("com.rtdi.bigdata.connector.ui.components.ErrorMessageItem", {
		metadata : {
			properties : {
				timestamp: "int",
				message: "string",
				exception: "string",
				stacktrace: "string",
				hint: "string",
				causingobject: "string",
				sourcecodeline: "string",
				errorhelp: "string",
				threadname: "string"
			},
			renderer: {}
		},
		init : function() {
			this.setType(sap.ui.core.MessageType.Error);
		},
		setStacktrace : function(value) {
			this.setProperty("stacktrace", value, true);
		},
		getStacktrace : function() {
			return this.getProperty("stacktrace");
		},
		setException : function(value) {
			this.setProperty("exception", value, true);
			this.setTitle(value);
			var oLink = new sap.m.Link({ text: "more...", press: [this.onShowErrorDetails,this] });
			this.setLink(oLink);
		},
		setMessage : function(value) {
			this.setProperty("message", value, true);
			this.setDescription(value);
		},
		setThreadname : function(value) {
			this.setProperty("threadname", value, true);
			this.setSubtitle(value);
		},
		onShowErrorDetails : function(oEvent, oData) {
			// this = is the Link control of a message item
			var oContext = this.getBindingContext();
			var oBindingObject = oContext.getObject();
			var oDialog = this._getDialog(oBindingObject);
			oDialog.open();
		},
		_getDialog : function(oBindingObject) {
			var oDetailsDialog = new sap.m.Dialog({
				title: "Error details",
				contentWidth: "900px",
				contentHeight: "600px",
				resizable: true,
				content: [
					new sap.ui.layout.form.SimpleForm({ width: "100%", content: [
						new sap.m.Title( { text: "Error" } ), 
						new sap.m.Label( { text: "Process" } ), 
						new sap.m.Text( { text: oBindingObject.threadname } ),
						new sap.m.Label( { text: "Exception type" } ), 
						new sap.m.Text( { text: oBindingObject.exception } ),
						new sap.m.Label( { text: "Time" } ), 
						new sap.m.Text( { text: new Date(oBindingObject.timestamp).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}) } ),
					] } ),

					new sap.ui.layout.form.SimpleForm({ width: "100%", content: [						
						new sap.m.Title( { text: "Messages" } ), 						
						new sap.m.Label( { text: "Message" } ), 
						new sap.m.Text( { text: oBindingObject.message } ),
						new sap.m.Label( { text: "Hint" } ), 
						new sap.m.Text( { text: oBindingObject.hint } ),
						new sap.m.Label( { text: "Help" } ), 
						new sap.m.Text( { text: oBindingObject.errorhelp } )
					] } ),

					new sap.ui.layout.form.SimpleForm({ width: "100%", content: [
						new sap.m.Title( { text: "Object in question" } ), 
						new sap.m.Label( { text: "Causing Object" } ), 
						new sap.m.Text( { text: oBindingObject.causingobject } ),
					] } ),

					new sap.ui.layout.form.SimpleForm({ width: "100%", content: [

						new sap.m.Title( { text: "Source code reference" } ), 
						new sap.m.Label( { text: "SourceCode" } ), 
						new sap.m.Link( { href: oBindingObject.sourcecodeline, text: "Show Source line", target: "_blank" } ),
					] } ),

					new sap.ui.layout.form.SimpleForm({ width: "100%", content: [
						
						new sap.m.Title( { text: "Stack trace" } ), 
						new sap.m.Label( { text: "Trace" } ), 
						new sap.m.Text( { text: oBindingObject.stacktrace } )
					] } )
				],
				endButton: new sap.m.Button({
					text: "Close",
					press: function () {
						oDetailsDialog.close();
					}.bind(this)
				})
			});
			return oDetailsDialog;
		}
	});
});
