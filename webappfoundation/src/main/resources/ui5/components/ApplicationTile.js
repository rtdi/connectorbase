sap.ui.define(["sap/ui/core/XMLComposite"], function(XMLComposite) {
	return XMLComposite.extend("com.rtdi.bigdata.connector.ui.components.ApplicationTile", {
		metadata: {
			properties: {
				name: "string",
				icon: "string",
				image: "string",
				href: "string",
				description: "string",
				tooltip: "string",
				target: "string"
			}
		},
	});
}, true);