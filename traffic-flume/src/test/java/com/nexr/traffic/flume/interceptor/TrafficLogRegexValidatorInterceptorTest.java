package com.nexr.traffic.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TrafficLogRegexValidatorInterceptorTest {

	private String regex = "^([\\d]{4}-[\\d]{2}-[\\d]{2}\\s[\\d]{2}:[\\d]{2}:[\\d]{2}),(ROAD_[0-9]+),(CAM_[0-9]+),([0-9]{2}[A-Z]{2}[0-9]{4}\\^[0-9]+\\|?)+";
	
	@Test
	public void testValidLog1() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:43:00,ROAD_1,CAM_34,69YK9562^72";
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context ctx = new Context();
        ctx.put("regex", this.regex);
        builder.configure(ctx);
        Interceptor interceptor = builder.build();

        Event event = EventBuilder.withBody(log, Charsets.UTF_8);
        event = interceptor.intercept(event);
        
        Assert.assertEquals("valid", event.getHeaders().get("validation"));
	}
	
	@Test
	public void testValidLog2() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:43:00,ROAD_1,CAM_34,69YK9562^72|94AY3251^79";
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context ctx = new Context();
		ctx.put("regex", this.regex);
		builder.configure(ctx);
		Interceptor interceptor = builder.build();

		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		event = interceptor.intercept(event);
		
		Assert.assertEquals("valid", event.getHeaders().get("validation"));
	}
	
	@Test
	public void testValidLog3() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:43:00,ROAD_1,CAM_34,69YK9562^72|94AY3251^79|94TP2308^71";
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context ctx = new Context();
		ctx.put("regex", this.regex);
		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		event = interceptor.intercept(event);
		
		Assert.assertEquals("valid", event.getHeaders().get("validation"));
	}
	
	@Test
	public void testValidLog4() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:43:00,ROAD_1,CAM_34,69YK9562^72|94AY3251^79|94TP2308^71|65QU0384^67";
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context ctx = new Context();
		ctx.put("regex", this.regex);
		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		event = interceptor.intercept(event);
		
		Assert.assertEquals("valid", event.getHeaders().get("validation"));
	}
	
	@Test
	public void testInValidLogType1() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:50, ROAD_27,CAM_94,03HQ9288^69";
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context ctx = new Context();
		ctx.put("regex", this.regex);
		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		event = interceptor.intercept(event);
		
		Assert.assertEquals("invalid", event.getHeaders().get("validation"));
	}
	
	@Test
	public void testInValidLogType2() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:50:00, ROAD_27,CAM_94,03HQ9288";
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context ctx = new Context();
		ctx.put("regex", this.regex);
		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		event = interceptor.intercept(event);
		
		Assert.assertEquals("invalid", event.getHeaders().get("validation"));
	}
	
	@Test
	public void testInValidLogType3() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:50:00, ROAD_27,CAM_94,69YK9562^72|94AY3251";
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context ctx = new Context();
		ctx.put("regex", this.regex);
		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		event = interceptor.intercept(event);
		
		Assert.assertEquals("invalid", event.getHeaders().get("validation"));
	}
	
	@Test
	public void testInValidLogType4() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:50:57, ROAD_28,CAM_81,2223RF234^66";
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context ctx = new Context();
		ctx.put("regex", this.regex);
		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		event = interceptor.intercept(event);
		
		Assert.assertEquals("invalid", event.getHeaders().get("validation"));
	}
	
	@Test
	public void testInValidLogType5() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:50:57, ROAD_27,CAM_7,70RR2364^akei";
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context ctx = new Context();
		ctx.put("regex", this.regex);
		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		event = interceptor.intercept(event);
		
		Assert.assertEquals("invalid", event.getHeaders().get("validation"));
	}
	
	@Test
	public void testInValidLogTypeDefault() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:50:57, ROAD_28,CAM_81,2223RF234^66";
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogRegexValidatorInterceptor$Builder");
		Context ctx = new Context();
		ctx.put("regex", this.regex);
		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		event = interceptor.intercept(event);
		
		Assert.assertEquals("invalid", event.getHeaders().get("validation"));
	}


}
