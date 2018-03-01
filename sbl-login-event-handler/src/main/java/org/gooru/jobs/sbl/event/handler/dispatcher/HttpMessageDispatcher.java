/**
 * 
 */
package org.gooru.jobs.sbl.event.handler.dispatcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * @author gooru
 *
 */
public final class HttpMessageDispatcher implements Dispatcher {

	private static final Logger LOGGER = LoggerFactory.getLogger(HttpMessageDispatcher.class);

	@Override
	public void dispatch(JsonObject eventBody) {
		try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
			final HttpPost postRequest = new HttpPost("");

			// Set Request headers
			postRequest.addHeader("Content-Type", "application/json");

			// Set request body
			final StringEntity requestPayload = new StringEntity(eventBody.toString());
			postRequest.setEntity(requestPayload);

			// Execute POST
			try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
				final int statusCode = response.getStatusLine().getStatusCode();
				HttpEntity responseEntity = response.getEntity();
				JsonObject responseBody = responseEntity != null ? readResponseBody(responseEntity.getContent())
						: new JsonObject();

				// Based on the HTTP status code build response object
				if (statusCode == 200) {
					LOGGER.info("HTTP POST success");
				} else {
					LOGGER.warn("HTTP POST failed with status code:{}, response:{}", statusCode, responseBody.toString());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static JsonObject readResponseBody(InputStream inputStream) throws IOException {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
			if (br.ready()) {
				StringBuilder sb = new StringBuilder();
				String line = null;
				while ((line = br.readLine()) != null) {
					sb.append(line);
				}
				return new JsonObject(sb.toString());
			} else {
				return new JsonObject();
			}
		}
	}

}
