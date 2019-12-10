# The UI and servlet classes

## Permissions

ServletSecurityConstants defines four roles:
* public static final String ROLE_VIEW = "connectorview"; 
* public static final String ROLE_SCHEMA = "connectorschema"; 
* public static final String ROLE_OPERATOR = "connectoroperator"; 
* public static final String ROLE_CONFIG = "connectorconfig"; 

The connectorview role allows to see all settings of a connector including their properties. But non of these can be modified, no new ones can be created. It does not allow to browse remote objects.
For browsing remote objects and creating new schemas the connectorschema role is required.
The connectoroperator roles allows the user to start/stop connections/producers/consumers.
And to create new connections/producers/consumers or modify their settings, the connectorconfig role is required.



## Icons

| Item       | Icon                         | Unicode |
| ---------- | ---------------------------- | ------- |
| Topic      | sap-icon://batch-payments    | e061    |
| Schema     | sap-icon://database          | e080    |
|            |                              |         |
| Pipeline   | sap-icon://pipeline-analysis | e143    |
| Connection | sap-icon://connected         | e20b    |
| Producer   | sap-icon://table-row         | e27e    |
| Consumer   | sap-icon://detail-view       | e1d8    |
| Service    | sap-icon://workflow-tasks    | e0e5    |
|            |                              |         |
|            |                              |         |
| Help       | sap-icon://sys-help          | e1c4    |





