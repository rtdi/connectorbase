sap.ui.define([ 'sap/ui/core/Control' ], function(Control) {
	
	var maxdepth = 10;
	
	function htmlEncode(value){
		  // Create a in-memory div, set its inner text (which jQuery automatically encodes)
		  // Then grab the encoded contents back out. The div never exists on the page.
		  return $('<div/>').text(value).html();
		};
	
	var addSubtable = function(json, outputtext, depth) {
		if (json == null || depth > maxdepth) {
			return outputtext;
		} else if (typeof json !== 'object') {
			return outputtext;
		} else {
			/*
			 * Three cases:
			 * 1. It is a sub-record like address.period
			 * 2. It is an array of records like patient.address[]
			 * 3. It is an array of data
			 */
			if (Array.isArray(json) && json.length == 0) {
				// do nothing; do not print an empty table. Thus losing the inner fields but...
				return outputtext;
			} else {
				if (depth == 0) {
					outputtext += "<table class='JSONViewerTable'>";
				} else {
					// Expand-able
					/*
					outputtext += "<img alt='+' src='./images/expand.png' onclick=\"if (this.nextElementSibling.style.display === 'none') "; 
					outputtext += "{ this.nextElementSibling.style.display = '' } "; 
					outputtext += "else { this.nextElementSibling.style.display = 'none' } \" >"; 
					*/
					outputtext += "<details>";
					outputtext += "<table class='JSONViewerSubTable'>";
				}
				
				// An array of objects or a subrecord both get headers. An array of primitives does not.
				if ((Array.isArray(json) && typeof json[0] === 'object') || !Array.isArray(json)) {
					// case 1 or 2 write an header, case 3 does not
					var header = json;
					if (Array.isArray(json)) {
						// need to loop through the first element, not the array for getting the header data.
						header = json[0];
					}
					outputtext += "<thead><tr class='JSONViewerTR'>";
					for (var cell in header) {
						outputtext += "<th class='JSONViewerTH' >";
						outputtext += cell;
						outputtext += "</th>";
					}
					outputtext += "</tr></thead>";
				}
				
				if (Array.isArray(json)) {
					for (var i in json) {
						var a = json[i];
						if (a == null) {
							
						} else if (typeof a === 'object') {
							// case 2
							outputtext = addRow(a, outputtext, depth);
						} else {
							// case 3
							outputtext += "<tr><td class='JSONViewerTD'>" + htmlEncode(a) + "</td></tr>";
						}
					}
				} else {
					// case 1
					outputtext = addRow(json, outputtext, depth);
				}
				outputtext += "</table>";
				if (depth != 0) {
					outputtext += "</details>";
				}
				return outputtext;
			}
		}
	};
	
	var addRow = function(json, outputtext, depth) {
		if (json == null) {
			return outputtext;
		} else if (typeof json !== 'object' || Array.isArray(json)) {
			return outputtext;
		} else {
			outputtext += "<tr>";
			
			for (var cell in json) {
				outputtext += "<td class='JSONViewerTD'>";
				var o = json[cell];
				if (o == null) {
					// nothing
				} else if (typeof o === 'object') {
					outputtext = addSubtable(o, outputtext, depth+1);
				} else {
					outputtext += htmlEncode(o);
				}
				outputtext += "</td>";
			}
			outputtext += "</tr>";
			return outputtext;
		}
	};

	return Control.extend("sap.ui.demo.wt.components.JSONViewer", {
		metadata : {
			properties : {
				text: 	{type : "string", defaultValue : ""}
			},
			aggregations : {},
		},

		renderer : function(oRm, oControl) {
			oRm.write("<div");
			oRm.writeControlData(oControl);
			oRm.write("><div class='JSONViewerDIV'>"); // will carry the scrollbar
			
			var outputtext = "";
			var json = JSON.parse( oControl.getText() );
			outputtext = addSubtable(json, outputtext, 0);
			
			oRm.write(outputtext);

			oRm.write("</div></div>")
		},

		init : function() {
			var libraryPath = jQuery.sap.getModulePath("sap.ui.demo.wt.components");
			jQuery.sap.includeStyleSheet(libraryPath + "/../css/JSONViewer.css");
		},
		
		onAfterRendering : function(arguments) {
			if (sap.ui.core.Control.prototype.onAfterRendering) {
				sap.ui.core.Control.prototype.onAfterRendering.apply(this, arguments);
			}
		},

	});
});
