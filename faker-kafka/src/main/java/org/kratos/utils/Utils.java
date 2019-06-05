package org.kratos.utils;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DateFormatUtils;
import org.json.JSONObject;

import com.github.javafaker.Faker;

public class Utils {
	
	static public Faker faker = null;

	/**
	 * singleton data faker
	 * 
	 * @return faker
	 */
	static public Faker getFaker() {
		if (faker == null)
			faker = new Faker(Locale.ENGLISH);

		return faker;
	}

	/**
	 * 
	 * @return
	 */
	static public JSONObject fakeJsonData(Faker faker) 
	{
		JSONObject json = new JSONObject();
		
		/*
		 * Random fields from data faker
		 */
		json.put("transaction_id", faker.number().randomNumber(7, true));
		json.put("card_number", faker.business().creditCardNumber());
		json.put("card_type", faker.business().creditCardType());
		json.put("currency", faker.currency().name());
		json.put("transaction_date", DateFormatUtils.format(faker.date().past(7, TimeUnit.DAYS), "yyyy-MM-dd"));
		json.put("validation_phone", faker.phoneNumber().cellPhone());
		json.put("city", faker.address().city());
		json.put("country", faker.address().countryCode());
		json.put("latitude", faker.address().latitude());
		json.put("longitude", faker.address().longitude());

		return json;
	}

}
