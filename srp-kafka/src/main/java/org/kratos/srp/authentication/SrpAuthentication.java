package org.kratos.srp.authentication;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

public class SrpAuthentication {
	
	
	private SrpAuthentication() {}
	
	public static SrpAuthentication getInstance() {
		if(srp == null)
			srp = new SrpAuthentication();
		
		return srp;
	}
	
	/**
	 * Domain name
	 * 
	 */
	final String DOMAIN = "https://api.structuredretailproducts.com";

	/**
	 * Public API key
	 * 
	 */
	final String PUBLIC_API_KEY = "";

	/**
	 * Private API key
	 * 
	 */
	final String PRIVATE_API_KEY = "";

	/**
	 * SHA1 Sign the string
	 * 
	 * @param baseString base string to be singed
	 * @return signed string with private key
	 * @throws Exception
	 */
	public String _hmacSha1(String baseString) throws Exception {

		String keyString = this.PRIVATE_API_KEY;
		SecretKey secretKey = null;

		byte[] keyBytes = keyString.getBytes();
		secretKey = new SecretKeySpec(keyBytes, "hmacSha1");

		Mac mac = Mac.getInstance("hmacSha1");

		mac.init(secretKey);

		byte[] text = baseString.getBytes();

		return new String(Base64.encodeBase64(mac.doFinal(text))).trim();
	}

	/**
	 * 
	 * @param strUri
	 * @param strContentLength
	 * @param strContentMD5
	 * @return
	 * @throws Exception
	 */
	protected String _getAuthHeader(String strUri, String strContentLength, String strContentMD5) throws Exception {

		long lngTimestamp = System.currentTimeMillis() / 1000L;
		String strStringToSign = "GET " + strUri + " " + strContentLength + " " + strContentMD5 + " " + lngTimestamp;
		String strSignature = this._hmacSha1(strStringToSign);
		String strAuth = "SRP " + this.PUBLIC_API_KEY + ":" + strSignature + ":" + lngTimestamp;
		return strAuth;
	}

	/**
	 * 
	 * @param strUri
	 * @return
	 */
	public String fetch(String strUri) {
		try {
			HttpClient client = HttpClientBuilder.create().build();
			HttpGet request = new HttpGet(this.DOMAIN + strUri);

			// for a get request strContentLength,strContentMD5 are always empty string, DO
			// NOT use null,
			// it will create a wrong signature key

			request.setHeader("Authorization", this._getAuthHeader(strUri, "", ""));

			HttpResponse response = client.execute(request);
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

			StringBuilder builder = new StringBuilder();
			String aux = "";

			while ((aux = reader.readLine()) != null) {
				builder.append(aux);
			}

			return builder.toString();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private static SrpAuthentication srp = null;

}
