package com.nexr.traffic.flume.interceptor;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class TrafficLogRegexValidatorInterceptor implements Interceptor {

	private static final Logger logger = LoggerFactory.getLogger(TrafficLogRegexValidatorInterceptor.class);
	
	private static final String VALIDATION = "validation";
	private static final String VALID = "valid";
	private static final String INVALID = "invalid";
	private static final String REGEX = "regex";
	private static final String DEFAULT_REGEX = ".*";
	
	private final Pattern regex;

	public TrafficLogRegexValidatorInterceptor(Pattern regex) {
		this.regex = regex;
	}

	@Override
	public void initialize() {
		// no-op
	}
	
	private boolean validate(Event event) {
		if ((event == null) || (event.getBody() == null)) {
			return false;
		}
		return this.regex.matcher(new String(event.getBody())).find();
	}

	@Override
	public Event intercept(Event event) {
		if (validate(event)) {
			event.getHeaders().put(VALIDATION, VALID);
		} else {
			event.getHeaders().put(VALIDATION, INVALID);
		}
		return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> out = Lists.newArrayList();
		for (Event event : events) {
			Event outEvent = intercept(event);
			if (outEvent != null) {
				out.add(outEvent);
			}
		}
		return out;
	}

	@Override
	public void close() {
		// no-op
	}

	public static class Builder implements Interceptor.Builder {

		private Pattern regex;

		@Override
		public void configure(Context context) {
			String regexString = context.getString(REGEX, DEFAULT_REGEX);
			regex = Pattern.compile(regexString);
		}

		@Override
		public Interceptor build() {
			logger.info(String.format("Creating TrafficLogRegexValidatorInterceptor: regex:%s", regex.toString()));
			return new TrafficLogRegexValidatorInterceptor(regex);
		}

	}

}
