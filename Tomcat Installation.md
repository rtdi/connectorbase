# Installing a RTDI Connector

The connectors are simple web applications. They come as web archive (war files) and can be deployed as is. The hosting web application server, most likely an Apache tomcat, has just a single requirement: It has to assign roles called connectorview, connectorschema, connectoroperator, connectorconfig to a user.

An optional but recommended task is to set the Servlet context environment variable io.rtdi.bigdata.connectors.configpath to a location where all the connectors and pipeline's config files shall be saved. If this is a directory outside the web application, upgrading a connector to a new version is as simple as uploading the war file again.

https://codippa.com/how-to-deploy-war-file-in-tomcat/

How to setup a tomcat with https, users is well documented but in different places. As a receipt, it is described here again.

## Tomcat installation cookbook via apt 

As most files and commands will work as root only, switch to the root user

*sudo su -*

Install tomcat9 and its admin pages via apt

*apt install tomcat9 tomcat9-admin*

To make sure the tomcat is not running while editing its configurations

*systemctl stop tomcat9*

Change the tomcat ports to the default http and https ports and prepare for installing the certificates

*vi /etc/tomcat9/server.xml* 

* change port from 8080 to 80 and redirect port from 8443 to 443

  ```xml
  <Connector port="80" protocol="HTTP/1.1"
             connectionTimeout="20000"
             redirectPort="443" />
  ```

* uncomment the 8443 Connector and change the port to 443

* modify the Certificate entries

  ```xml
  <Connector port="443" protocol="org.apache.coyote.http11.Http11AprProtocol"
             maxThreads="20" SSLEnabled="true" isSecure="true">
      <UpgradeProtocol className="org.apache.coyote.http2.Http2Protocol" />
      <SSLHostConfig>
          <Certificate certificateKeyFile="conf/cert/privkey.pem"
                       certificateFile="conf/cert/cert.pem"
                       certificateChainFile="conf/cert/fullchain.pem"
                       type="RSA" />
      </SSLHostConfig>
  </Connector>
  ```



In order to use certificates installed via let's encrypt, their repository needs to be added.

*add-apt-repository ppa:certbot/certbot*

*apt update*

*apt install certbot*

Request certificates for the host, in this example it is the host connectors.rtdi.io. The certbot will spin up its own webserver (--standalone) to validate the host name. If another webserver is running at port 80 at that time, this will not work.

*certbot certonly --standalone -d connectors.rtdi.io*

The new certificate files need to be made known to the tomcat and have to be read-able by the tomcat user.

*mkdir /var/lib/tomcat9/conf/cert*

*ln -s /etc/letsencrypt/live/\*/cert.pem /var/lib/tomcat9/conf/cert/cert.pem*
*ln -s /etc/letsencrypt/live/\*/fullchain.pem /var/lib/tomcat9/conf/cert/fullchain.pem*
*ln -s /etc/letsencrypt/live/\*/chain.pem /var/lib/tomcat9/conf/cert/chain.pem*
*ln -s /etc/letsencrypt/live/connectors.rtdi.io/privkey.pem /var/lib/tomcat9/conf/cert/privkey.pem*

*chgrp tomcat /var/lib/tomcat9/conf/cert*

*chmod 770 /var/lib/tomcat9/conf/cert*

*chmod g+rx /etc/letsencrypt/archive*
*chmod g+rx /etc/letsencrypt/archive/\**
*chmod g+rx /etc/letsencrypt/live*
*chmod g+rx /etc/letsencrypt/live/\**

*chgrp tomcat /etc/letsencrypt/archive*
*chgrp tomcat /etc/letsencrypt/archive/\**
*chgrp tomcat /etc/letsencrypt/live*
*chgrp tomcat /etc/letsencrypt/live/\**

Next step is to configure users in tomcat and to assign them roles. Depending on the IDP to be used there are different ways. The most basic option is to use tomcat's config file. The tomcat-users.xml should contain the xml below but with different passwords.

*vi /var/lib/tomcat/conf/tomcat-users.xml*

```xml
<role rolename="manager-gui" />
<user username="connectoradmin" password="change!!" roles="connectorview, connectorschema, connectoroperator, connectorconfig"/>
<user username="connectoruser" password="change!!" roles="connectorview"/>
<user username="tomcatadmin" password="change!!" roles="manager-gui"/>
```

No webpage should use http but https instead. So if a browser happens to use the http protocol, instruct the tomcat server to redirect to https for all pages. Add below xml snippet right before the closing web-app tag in the servers web.xml.

*vi /var/lib/tomcat/conf/web.xml*

```xml
<!-- Force HTTPS, required for HTTP redirect! -->
<security-constraint>
    <web-resource-collection>
        <web-resource-name>Protected Context</web-resource-name>
        <url-pattern>/*</url-pattern>
    </web-resource-collection>
    <!-- auth-constraint goes here if you require authentication -->
    <user-data-constraint>
        <transport-guarantee>CONFIDENTIAL</transport-guarantee>
    </user-data-constraint>
</security-constraint>
```

By default all settings made via the Connectors UI will be stored in the webapp's WEB-INF directory. This is fine until a new version of the webapp is installed, because that means deleting the entire webapp and re-deploying it. Hence all settings would be lost. Therefore within the tomcat a global environment variable can be set. Each connector will create a sub directory and store the settings there.

*vi /var/lib/tomcat/conf/context.xml add*

```xml
<Environment name="io.rtdi.bigdata.connectors.configpath" value="/var/lib/tomcat9/conf/rtdiconfig" type="java.lang.String"/>
```

Finally the tomcat can be started again and hopefully the logs in /var/lib/tomcat/logs show no errors.

*systemctl start tomcat9*

Exit the root user

*exit*





