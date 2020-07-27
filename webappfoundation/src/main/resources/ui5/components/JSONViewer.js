sap.ui.define(
[ 'sap/ui/core/Control' ],
function(Control) {

	function htmlEncode(value) {
		// Create a in-memory div, set its inner text (which
		// jQuery automatically encodes)
		// Then grab the encoded contents back out. The div
		// never exists on the page.
		return $('<div/>').text(value).html();
	};

	/**
	 * Check if arg is either an array with at least 1 element, or a dict with at least 1 key
	 * @return boolean
	 */
	function isCollapsable(arg) {
		return arg instanceof Object && Object.keys(arg).length > 0;
	};

	/**
	 * Check if a string represents a valid url
	 * @return boolean
	 */
	function isUrl(string) {
		// From https://stackoverflow.com/questions/5717093/check-if-a-javascript-string-is-a-url
		var pattern = new RegExp('^(https?:\\/\\/)?' + // protocol
				'((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|' + // domain name
				'((\\d{1,3}\\.){3}\\d{1,3}))' + // OR ip (v4) address
				'(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*' + // port and path
				'(\\?[;&a-z\\d%_.~+=-]*)?' + // query string
				'(\\#[-a-z\\d_]*)?$', 'i'); // fragment locator
		return pattern.test(string);
	};
	
	var counter = 0;

	/**
	 * Transform a json object into html representation
	 * 
	 * @return string
	 */
	function json2html(json, controlid, isRoot) {
		var html = '';
		if (typeof json === 'string') {
			// Escape tags and quotes
			json = json.replace(/&/g, '&amp;').replace(/</g,
					'&lt;').replace(/>/g, '&gt;').replace(/'/g,
					'&apos;').replace(/"/g, '&quot;');

			// Escape double quotes in the rendered non-URL
			// string.
			json = json.replace(/&quot;/g, '\\&quot;');
			html += '<span class="json-string">"' + json
					+ '"</span>';
		} else if (typeof json === 'number') {
			html += '<span class="json-literal">' + json
					+ '</span>';
		} else if (typeof json === 'boolean') {
			html += '<span class="json-literal">' + json
					+ '</span>';
		} else if (json === null) {
			html += '<span class="json-literal">null</span>';
		} else if (json instanceof Array) {
			if (json.length > 0) {
				var placeholder = json.length + (json.length > 1 ? ' items' : ' item');
				html += '<a class="json-toggle" href="#" id="' + controlid + '_' + counter + '"></a>';
				html += '[<ol class="json-array" style="display: none;">';
				counter++;
				for (var i = 0; i < json.length; ++i) {
					html += '<li>';
					html += json2html(json[i], controlid, false);
					// Add comma if item is not last
					if (i < json.length - 1) {
						html += ',';
					}
					html += '</li>';
				}
				html += '</ol>';
				html += '<a class="json-placeholder" href="#" >' + placeholder + '</a>';
				html += ']';
			} else {
				html += '[]';
			}
		} else if (typeof json === 'object') {
			var keyCount = Object.keys(json).length;
			var placeholder = keyCount + (keyCount > 1 ? ' items' : ' item');
			if (keyCount > 0) {
				html += '{<ul class="json-dict">';
				for ( var key in json) {
					if (Object.prototype.hasOwnProperty.call(json, key)) {
						html += '<li>';
						html += '<span class="json-string">"' + key + '"</span>';
						html += ': ' + json2html(json[key], controlid, false);
						// Add comma if item is not last
						if (--keyCount > 0) {
							html += ',';
						}
						html += '</li>';
					}
				}
				html += '</ul>';
				html += '}';
			} else {
				html += '{}';
			}
		}
		return html;
	};

      
	var control = Control
			.extend(
					"com.rtdi.bigdata.connector.ui.components.JSONViewer",
					{
						metadata : {
							properties : {
								text : {
									type : "string",
									defaultValue : ""
								}
							},
							aggregations : {},
						},

						renderer : function(oRm, oControl) {
							oRm.write("<div");
							oRm.writeControlData(oControl);
							oRm.write("><div class='JSONViewerDIV'>"); // will carry the scrollbar

							var outputtext = "";
							var json = JSON.parse(oControl.getText());
							outputtext = json2html(json, oControl.getId(), true);

							oRm.write(outputtext);

							oRm.write("</div></div>")
						},

						init : function() {
							var libraryPath = jQuery.sap.getModulePath("com.rtdi.bigdata.connector.ui.components");
							jQuery.sap.includeStyleSheet(libraryPath + "/JSONViewer.css");
						},

						onAfterRendering : function(arguments) {
							if (sap.ui.core.Control.prototype.onAfterRendering) {
								sap.ui.core.Control.prototype.onAfterRendering.apply(this, arguments);
							}
						},

					});
	
	control.prototype.onclick = function (oEvent) {
		var target = $(oEvent.target).siblings('ul.json-dict, ol.json-array');
		target.toggle();
		if (target.is(':visible')) {
			target.siblings('.json-placeholder').remove();
		} else {
			var count = target.children('li').length;
			var placeholder = count + (count > 1 ? ' items' : ' item');
			target.after('<a class="json-placeholder" href="#" >' + placeholder + '</a>');
		}
		return false;
	};

	return control;
});
