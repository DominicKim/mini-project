package com.nexr.traffic.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TrafficDedupInterceptorTest {

	@Test
	public void test() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:43:00,ROAD_1,CAM_34,69YK9562^72|94AY3251^79";
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficDedupInterceptor$Builder");
		Context ctx = new Context();
		ctx.put("cache.size", "2");
		builder.configure(ctx);
		Interceptor interceptor = builder.build();

		Event resEvent = interceptor.intercept(event);
		Assert.assertNotNull(resEvent);
		Assert.assertEquals(log, new String(resEvent.getBody()));
		Assert.assertNull(interceptor.intercept(event));
		Assert.assertNull(interceptor.intercept(event));
	}

}
