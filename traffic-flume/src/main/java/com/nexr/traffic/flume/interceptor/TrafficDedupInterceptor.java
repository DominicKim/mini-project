package com.nexr.traffic.flume.interceptor;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.nexr.traffic.flume.common.FixedArrayList;

public class TrafficDedupInterceptor implements Interceptor {

	private static final Logger logger = LoggerFactory.getLogger(TrafficDedupInterceptor.class);

	private FixedArrayList<String> dedupList;
	
	public TrafficDedupInterceptor(Integer cacheSize) {
		this.dedupList = new FixedArrayList<String>(cacheSize.intValue());
	}

	@Override
	public void initialize() {
		// no-op
	}

	@Override
	public Event intercept(Event event) {
	    String body = new String(event.getBody());
	    if (!(this.dedupList.contains(body))) {
	      this.dedupList.add(body);
	      return event;
	    }
	    return null;
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

		private Integer cacheSize;

		@Override
		public void configure(Context context) {
			cacheSize = context.getInteger("cache.size", Integer.valueOf(100));
		}

		@Override
		public Interceptor build() {
			logger.info(String.format("Creating TrafficDedupInterceptor: cache.size:%d", cacheSize));
			return new TrafficDedupInterceptor(cacheSize);
		}
	}

}
