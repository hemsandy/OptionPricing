package com.wf.option.pricing.model;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;


public class OptionData {
	private String stockName;
	private String optionName;
	private double strike;
	private double volatility;
	private LocalDate expiryDate;
	private double stockPrice;
	private double optionPrice;

	public OptionData(){
		
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Stock Name : "+stockName + " -- ");
		sb.append("Contract Name : "+optionName + " -- ");
		sb.append("Expiry Date : "+expiryDate + " -- ");
		sb.append("Strike : "+strike + " -- ");
		sb.append("volatility : "+volatility + " -- ");
		sb.append("stockPrice : "+stockPrice + " -- ");
		sb.append("optionPrice : "+optionPrice + " -- ");
		return sb.toString();
	}

	public OptionData(String stockName, String optionName, double strike, double voloatility,
					  LocalDate expiry,double stockPrice, double optionPrice){
		this.stockName = stockName;
		this.optionName = optionName;
		this.strike = strike;
		this.volatility = voloatility;
		this.expiryDate = expiry;
		this.stockPrice = stockPrice;
		this.optionPrice = optionPrice;
		this.stockName = optionName.substring(0,4);
	}

	public String getStockName() {
		return stockName;
	}

	public void setStockName(String stockName) {
		this.stockName = stockName;
	}

	public String getOptionName() {
		return optionName;
	}

	public void setOptionName(String optionName) {
		this.optionName = optionName;
		this.stockName = optionName.substring(0,4);
	}

	public double getStrike() {
		return strike;
	}

	public void setStrike(double strike) {
		this.strike = strike;
	}

	public double getVolatility() {
		return volatility;
	}

	public void setVolatility(double volatility) {
		this.volatility = volatility;
	}

	public LocalDate getExpiryDate() {
		return expiryDate;
	}

	public void setExpiryDate(LocalDate expiryDate) {
		this.expiryDate = expiryDate;
	}

	public static DateTimeFormatter formatter =  DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH][:mm][:mm][ SSS]");

	public double getStockPrice() {
		return stockPrice;
	}

	public void setStockPrice(double stockPrice) {
		this.stockPrice = stockPrice;
	}

	public double getOptionPrice() {
		return optionPrice;
	}

	public void setOptionPrice(double optionPrice) {
		this.optionPrice = optionPrice;
	}

	public static Gson gson  = new GsonBuilder()
			.registerTypeAdapter(LocalDate.class, new JsonSerializer<LocalDate>(){

				public JsonElement serialize(LocalDate localDateTime, Type typeOfSrc,
											 JsonSerializationContext context) {
					return new JsonPrimitive(formatter.format(localDateTime));
				}
			}).registerTypeAdapter(LocalDate.class, new JsonDeserializer<LocalDate>() {
				public LocalDate deserialize(JsonElement json, Type typeOfT,
												 JsonDeserializationContext context) throws JsonParseException {
					LocalDate dt = LocalDate.parse(json.getAsString(),formatter);
					return dt;
				}
			}).create();
	public String toJSONString() {

		return gson.toJson(this);
	}

	public static OptionData fromJsonString(String jsonObjectStr) {
		OptionData optionData = null;
		try {
			optionData = gson.fromJson(jsonObjectStr, OptionData.class);

		}catch(Exception e) {
			System.out.println("Parse Exception :" + e.getMessage() + "forMessage:" + jsonObjectStr);
		}

		return optionData;
	}

}
