package com.wf.option.pricing;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import com.wf.option.pricing.model.OptionData;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jquantlib.Settings;
import org.jquantlib.daycounters.Actual365Fixed;
import org.jquantlib.daycounters.DayCounter;
import org.jquantlib.exercise.EuropeanExercise;
import org.jquantlib.exercise.Exercise;
import org.jquantlib.instruments.EuropeanOption;
import org.jquantlib.instruments.Option;
import org.jquantlib.instruments.Payoff;
import org.jquantlib.instruments.PlainVanillaPayoff;
import org.jquantlib.instruments.VanillaOption;
import org.jquantlib.pricingengines.AnalyticEuropeanEngine;
import org.jquantlib.processes.BlackScholesMertonProcess;
import org.jquantlib.quotes.Handle;
import org.jquantlib.quotes.Quote;
import org.jquantlib.quotes.SimpleQuote;
import org.jquantlib.termstructures.BlackVolTermStructure;
import org.jquantlib.termstructures.YieldTermStructure;
import org.jquantlib.termstructures.volatilities.BlackConstantVol;
import org.jquantlib.termstructures.yieldcurves.FlatForward;
import org.jquantlib.time.Calendar;
import org.jquantlib.time.Date;
import org.jquantlib.time.calendars.Target;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptionPricerBolt extends BaseBasicBolt {
	private static Logger logger = LoggerFactory.getLogger("OptionPricerBolt");

	private String name;

	private boolean autoAck;

	public OptionPricerBolt(String name, boolean autoAck){
		this.name = name;
		this.autoAck = autoAck;

	}


	public void execute(Tuple tuple, BasicOutputCollector collector) {
//		logger.info("--------------------------------------------------"+System.nanoTime());
		List<Object> values = tuple.getValues();
		OptionData  optionData = (OptionData) values.get(0);
//		OptionData optionData = OptionData.fromJsonString((String) values.get(0));
		double  underlyingPrice = optionData.getStockPrice();
		String  optionName = (String)values.get(1);

		//logger.info("Inside execute method of OptionPricerBolt : underlyingPrice :"+underlyingPrice);
		try {
			double optionPrice = price(optionData, underlyingPrice);
			optionData.setOptionPrice(optionPrice);
			optionData.setLastUpdatedTime(LocalDateTime.now());
			logger.info("Final price calculated for option : {} is : {} : {}", optionData.getOptionName(), optionPrice, optionData.getBatchId());
			//logger.info("------------------------End--------------------------"+System.nanoTime());

			//if(this.declaredFields != null){
				//logger.info("[{}] emitting: {} with optionPrice: {}", this.name, tuple,optionPrice);
				collector.emit(new Values(optionData,optionName));
			//}
		}catch(Exception e) {
			logger.error("Failed to price the option {}", optionData.getOptionName(),e);
		}

	}

	private double price(OptionData optionData, double underlying) {

		final Option.Type type = Option.Type.Call;
		//String optionName = optionData.getOptionName();
		/* @Rate */final double riskFreeRate = 0.0256;
		double impVol = optionData.getVolatility();
		if (impVol == 0.0)
			impVol = 1;
		final double volatility = impVol;
		final double dividendYield = 0.00;
		// set up dates
		final Calendar calendar = new Target();
		final Date todaysDate = new Date(new java.util.Date());
		new Settings().setEvaluationDate(todaysDate);
		LocalDate localDate = optionData.getExpiryDate();
		Date expiryDate = new Date(java.util.Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant()));

		final DayCounter dayCounter = new Actual365Fixed();
		final Exercise europeanExercise = new EuropeanExercise(expiryDate);

		// bootstrap the yield/dividend/volatility curves
		final Handle<Quote> underlyingH = new Handle<Quote>(new SimpleQuote(
				underlying));
		final Handle<YieldTermStructure> flatDividendTS = new Handle<YieldTermStructure>(
				new FlatForward(todaysDate, dividendYield, dayCounter));
		final Handle<YieldTermStructure> flatTermStructure = new Handle<YieldTermStructure>(
				new FlatForward(todaysDate, riskFreeRate, dayCounter));
		final Handle<BlackVolTermStructure> flatVolTS = new Handle<BlackVolTermStructure>(
				new BlackConstantVol(todaysDate, calendar, volatility,
						dayCounter));
		final Payoff payoff = new PlainVanillaPayoff(type, optionData.getStrike());

		final BlackScholesMertonProcess bsmProcess = new BlackScholesMertonProcess(
				underlyingH, flatDividendTS, flatTermStructure, flatVolTS);

		// European Options
		final VanillaOption europeanOption = new EuropeanOption(payoff,
				europeanExercise);

		europeanOption.setPricingEngine(new AnalyticEuropeanEngine(bsmProcess));
		// Black-Scholes for European
		return europeanOption.NPV();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("optionDataWithPrice","optionName"));
	}

}
