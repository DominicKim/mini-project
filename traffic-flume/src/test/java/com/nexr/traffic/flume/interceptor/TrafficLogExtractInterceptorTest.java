package com.nexr.traffic.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.protobuf.TextFormat.ParseException;
import com.nexr.traffic.flume.common.TrafficConstants;

public class TrafficLogExtractInterceptorTest {

	private String regex = "^([\\d]{4}-[\\d]{2}-[\\d]{2}\\s[\\d]{2}:[\\d]{2}:[\\d]{2}),(ROAD_[0-9]+),(CAM_[0-9]+),([0-9]{2}[A-Z]{2}[0-9]{4}\\^[0-9]+\\|?)+";
	
	@Test
	public void testEvent() throws ClassNotFoundException, InstantiationException, IllegalAccessException, ParseException {
		String log = "2013-08-19 19:43:00,ROAD_1,CAM_34,69YK9562^72|94AY3251^79|94TP2308^71|65QU0384^67";
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		
		Interceptor.Builder validBuilder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context validCtx = new Context();
		validCtx.put("regex", this.regex);
        validBuilder.configure(validCtx);
        Interceptor validInterceptor = validBuilder.build();
        event = validInterceptor.intercept(event);
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogExtractInterceptor$Builder");
        
		Context ctx = new Context();
        builder.configure(ctx);
        
        Interceptor interceptor = builder.build();
        event = interceptor.intercept(event);
        
        Assert.assertEquals("2013-08-19", event.getHeaders().get(TrafficConstants.HEADER_DATE));
        Assert.assertEquals("19", event.getHeaders().get(TrafficConstants.HEADER_HOUR));
        Assert.assertEquals("43,ROAD_1,CAM_34,69YK9562^72|94AY3251^79|94TP2308^71|65QU0384^67", new String(event.getBody()));
	}

}
