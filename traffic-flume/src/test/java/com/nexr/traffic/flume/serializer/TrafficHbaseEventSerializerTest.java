package com.nexr.traffic.flume.serializer;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TrafficHbaseEventSerializerTest {

	@Test
	public void test() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-19 19:43:00,ROAD_1,CAM_34,69YK9562^72|94AY3251^85|94TP2308^71|65QU0384^67";
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogExtractInterceptor$Builder");
        
		Context ctx = new Context();
        builder.configure(ctx);
        
        Interceptor interceptor = builder.build();
        event = interceptor.intercept(event);
        System.out.println(new String(event.getBody()));
        
        TrafficHbaseEventSerializer thes = new TrafficHbaseEventSerializer();
        Context context = new Context();
        context.put("overspeed", "70");
        thes.configure(context);
        thes.initialize(event, Bytes.toBytes("cf"));
        List<Row> rows = thes.getActions();
        for (Row row : rows) {
			System.out.println(row);
		}
	}

}
