package com.hncy58.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/**
 * HTTP调用工具类
 * @author tdz
 * @company hncy58 长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2018年8月25日 下午5:00:59
 */
public class HttpUtils {

	private static CloseableHttpClient defaultHttpClient = null;

	public static String doGet(String url, Map<String, String> headerMap) throws IOException {
		String result;
		defaultHttpClient = HttpClients.createDefault();
		HttpGet get = new HttpGet(url);
		setHeader(get, headerMap);
		result = EntityUtils.toString(defaultHttpClient.execute(get).getEntity(), "utf-8");
		return result;
	}

	public static String doGet(String url) throws IOException {
		String result;
		defaultHttpClient = HttpClients.createDefault();
		HttpGet get = new HttpGet(url);
		result = EntityUtils.toString(defaultHttpClient.execute(get).getEntity(), "utf-8");
		return result;
	}
	
	public static OutputStream doGetForStream(String url, Map<String, String> headerMap) throws IOException {
		defaultHttpClient = HttpClients.createDefault();
		HttpGet get = new HttpGet(url);
		setHeader(get, headerMap);
		CloseableHttpResponse response = defaultHttpClient.execute(get);
		HttpEntity entity = null;
		ByteArrayOutputStream bio = null;

		try {
			entity = response.getEntity();
			// EntityUtils.consume(entity);
			InputStream in = entity.getContent();
			byte[] bytes = new byte[1024];
			bio = new ByteArrayOutputStream();
			int len = 0;

			while ((len = in.read(bytes)) > 0) {
				bio.write(bytes, 0, len);
			}

			bio.flush();
		} finally {
			response.close();
		}
		return bio;
	}
	
	public static OutputStream doGetForStream(String url) throws IOException {
		
		defaultHttpClient = HttpClients.createDefault();
		HttpGet get = new HttpGet(url);
		CloseableHttpResponse response = defaultHttpClient.execute(get);
		HttpEntity entity = null;
		ByteArrayOutputStream bio = null;

		try {
			entity = response.getEntity();
			// EntityUtils.consume(entity);
			InputStream in = entity.getContent();
			byte[] bytes = new byte[1024];
			bio = new ByteArrayOutputStream();
			int len = 0;

			while ((len = in.read(bytes)) > 0) {
				bio.write(bytes, 0, len);
			}

			bio.flush();
		} finally {
			response.close();
		}
		return bio;
	}

	public static String doPost(String url, String data) throws IOException {
		String result;
		HttpPost post = new HttpPost(url);
		post.setEntity(new StringEntity(data, "utf-8"));
		defaultHttpClient = HttpClients.createDefault();
		result = EntityUtils.toString(defaultHttpClient.execute(post).getEntity(), "utf-8");
		return result;
	}

	public static String doPost(String url, Map<String, String> headerMap, String data) throws IOException {
		String result;
		HttpPost post = new HttpPost(url);
		setHeader(post, headerMap);
		post.setEntity(new StringEntity(data, "utf-8"));
		defaultHttpClient = HttpClients.createDefault();
		result = EntityUtils.toString(defaultHttpClient.execute(post).getEntity(), "utf-8");
		return result;
	}

	public static Object doPost(String url, Map<String, String> paramMap) throws Exception {

		CloseableHttpClient httpclient = HttpClients.createDefault();

		HttpPost httpPost = new HttpPost(url);

		List<NameValuePair> nvps = new ArrayList<NameValuePair>();
		paramMap.forEach((k, v) -> nvps.add(new BasicNameValuePair(k, v)));

		httpPost.setEntity(new UrlEncodedFormEntity(nvps, "UTF-8"));

		CloseableHttpResponse response = httpclient.execute(httpPost);

		HttpEntity entity = null;
		ByteArrayOutputStream bio = null;

		try {
			entity = response.getEntity();
			// EntityUtils.consume(entity);
			InputStream in = entity.getContent();
			byte[] bytes = new byte[1024];
			bio = new ByteArrayOutputStream();
			int len = 0;

			while ((len = in.read(bytes)) > 0) {
				bio.write(bytes, 0, len);
			}

			bio.flush();
		} finally {
			response.close();
		}

		return bio;
	}

	public static Object doPost(String url, Map<String, String> headerMap, Map<String, String> paramMap)
			throws Exception {

		CloseableHttpClient httpclient = HttpClients.createDefault();

		HttpPost httpPost = new HttpPost(url);

		List<NameValuePair> nvps = new ArrayList<NameValuePair>();
		paramMap.forEach((k, v) -> nvps.add(new BasicNameValuePair(k, v)));

		httpPost.setEntity(new UrlEncodedFormEntity(nvps, "UTF-8"));

		setHeader(httpPost, headerMap);

		CloseableHttpResponse response = httpclient.execute(httpPost);
		HttpEntity entity = null;
		ByteArrayOutputStream bio = null;

		try {
			entity = response.getEntity();
			// EntityUtils.consume(entity);
			InputStream in = entity.getContent();
			byte[] bytes = new byte[1024];
			bio = new ByteArrayOutputStream();
			int len = 0;

			while ((len = in.read(bytes)) > 0) {
				bio.write(bytes, 0, len);
			}

			bio.flush();
		} finally {
			response.close();
		}

		return bio;
	}

	private static void setHeader(HttpRequestBase httpRequestBase, Map<String, String> headers) {
		headers.forEach((k, v) -> httpRequestBase.setHeader(k, v));
	}

}
