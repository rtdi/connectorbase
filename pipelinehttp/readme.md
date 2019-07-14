## PipelineHTTP

This is an implementation of the PipelineAPI to work with the PipelineHTTPServer.

Instead of using a PipelineAPI implementation locally, with this the PipelineHTTPServer can be configured to us a certain implementation and locally all calls are directed to the server.

To work with proxy servers and firewalls the local service act like a browser. A connection with the remote server is established and then data is either uploaded (produced) or downloaded (consumed).

Unlike a regular API call like a Rest call, this implementation does stream the data for performance.