/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.web;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * ClassName: IgnoreSslCertRequestFactory Function: TODO
 *
 * @Author: dtang
 * @Date: 11/3/15, 1:56 PM
 */

@Configuration
public class IgnoreSslCertRequestFactory {
	@Bean
	Registry<ConnectionSocketFactory> socketFactoryRegistry() {
		SSLContextBuilder builder = SSLContexts.custom();
		try {
			builder.loadTrustMaterial(null, new TrustStrategy() {
				@Override
				public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
					return true;
				}
			});
			SSLContext sslContext = builder.build();
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
					SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			return RegistryBuilder.<ConnectionSocketFactory>create().register("https", sslsf).register("http", PlainConnectionSocketFactory.INSTANCE).build();
		} catch (Throwable throwable) {
			System.out.println(throwable);
		}
		return null;
	}

}

//	public IgnoreSslCertRequestFactory(HttpClient httpClient) {
//		super(httpClient);
//		setAllTrusted(getHttpClient());
//	}
//
//	public void setAllTrusted(HttpClient httpClient) {
//
//		SSLConnectionSocketFactory sslConnectionSocketFactory = null;
//
//		try {
//			sslConnectionSocketFactory = new SSLConnectionSocketFactory();
//			sslsf = new SSLSocketFactory(new TrustStrategy() {
//
//				public boolean isTrusted(final X509Certificate[] chain, String authType) throws CertificateException {
//					return true;
//				}
//
//			}, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
//		} catch (KeyManagementException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
//		}
//
//		httpClient.getConnectionManager().getSchemeRegistry().register(new Scheme("https", 443, sslsf));
//		h
//
//	}
//
//	//HttpComponentsClientHttpRequestFactory don't allow request body in DELETE method.
//	//Customize the http request for DELETE method to allow request body.
//	@Override
//	protected HttpUriRequest createHttpUriRequest(HttpMethod httpMethod, URI uri) {
//		if (HttpMethod.DELETE == httpMethod) {
//			return new HttpEntityEnclosingDeleteRequest(uri);
//		}
//		return super.createHttpUriRequest(httpMethod, uri);
//	}
//}
//
//class HttpEntityEnclosingDeleteRequest extends HttpEntityEnclosingRequestBase {
//	public HttpEntityEnclosingDeleteRequest(final URI uri) {
//		super();
//		setURI(uri);
//	}
//
//	@Override
//	public String getMethod() {
//		return "DELETE";
//	}
//}
