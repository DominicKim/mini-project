package com.nexr.traffic.flume.interceptor;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.nexr.traffic.flume.common.TrafficConstants;

public class TrafficLogExtractInterceptor implements Interceptor {

	private static final Logger logger = LoggerFactory.getLogger(TrafficLogExtractInterceptor.class);
	private final String FIELD_DELIMITER = ",";
	private final String TIMESTAMP_DELIMITER = " ";
	private final String TIME_DELIMITER = ":";
		
	@Override
	public void initialize() {
		// no-op
	}

	@Override
	public Event intercept(Event event) {
	    String body = new String(event.getBody());
	    String[] fields = body.split(FIELD_DELIMITER);
	    String[] dateTimeStr = fields[0].split(TIMESTAMP_DELIMITER);
	    String[] timeStr = dateTimeStr[1].split(TIME_DELIMITER);

	    event.getHeaders().put(TrafficConstants.HEADER_DATE, dateTimeStr[0]);
	    event.getHeaders().put(TrafficConstants.HEADER_ROAD, fields[1].split("_")[1]);

	    String cam = fields[2].split("_")[1];
	    String carInfo = fields[3];
	    
	    // hour, min, cam, car_info
	    String tempBody = timeStr[0] + FIELD_DELIMITER + timeStr[1] + FIELD_DELIMITER + cam + FIELD_DELIMITER + carInfo;
	    
	    event.setBody(tempBody.getBytes());
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

		@Override
		public void configure(Context context) {
		}

		@Override
		public Interceptor build() {
			logger.info("Creating TrafficLogExtractInterceptor");
			return new TrafficLogExtractInterceptor();
		}

	}
}
