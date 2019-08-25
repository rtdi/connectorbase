package io.rtdi.bigdata.connector.pipeline.foundation.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class HttpUtil {

	public static final String COOKIES_HEADER = "Set-Cookie";

	public static SSLContext sslcontext;
	public static HostnameVerifier verifier;
	private String encoded;
	private HttpURLConnection conn = null;
	private String cookieheader = null;
	
	static {
		try {
			verifier = new SSLHostnameVerifierNoop();
			// HttpsURLConnection.setDefaultHostnameVerifier(verifier);
	    	sslcontext = SSLContext.getInstance("TLS");
			sslcontext.init(null, new TrustManager[]{new SSLTrustManagerNoop()}, new java.security.SecureRandom());
		} catch (KeyManagementException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	

	public HttpUtil(String username, String password) throws PropertiesException {
		encoded = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
	}

	public void getHttpConnection(URL url, String requestmethod) throws IOException {
		conn  = (HttpURLConnection) url.openConnection();
		if (conn instanceof HttpsURLConnection) {
			((HttpsURLConnection) conn).setSSLSocketFactory(sslcontext.getSocketFactory());
			((HttpsURLConnection) conn).setHostnameVerifier(verifier);
		}
		conn.setRequestMethod(requestmethod);
		conn.setDoOutput(true);
		conn.setDoInput(true);
		conn.setReadTimeout(60000);
		conn.setRequestProperty("Authorization", "Basic " + encoded);
		if (cookieheader != null) {
		    conn.setRequestProperty("Cookie", cookieheader);    
		}
	}
	
	public HttpURLConnection getConnection() {
		return conn;
	}
	
	public SSLContext getSSLContext() {
		return sslcontext;
	}
	
	public HostnameVerifier getHostnameVerifier() {
		return verifier;
	}
	
	public void updateCookies() {
		Map<String, List<String>> headerFields = conn.getHeaderFields();
		List<String> cookiesHeader = headerFields.get(COOKIES_HEADER);

		if (cookiesHeader != null) {
			StringBuffer b = new StringBuffer();
		    for (String cookie : cookiesHeader) {
		    	if (b.length() != 0) {
		    		b.append(',');
		    	}
		    	b.append(cookie);
		    }               
		    cookieheader = b.toString();
		}
	}
}
