package com.nexr.traffic.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TrafficHiveAddPartitionInterceptorTest {

//	@Test
	@Ignore
	public void test() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String log = "2013-08-21 19:43:00,ROAD_2,CAM_34,69YK9562^72|94AY3251^79|94TP2308^71|65QU0384^67";
		Event event = EventBuilder.withBody(log, Charsets.UTF_8);
		
		Interceptor.Builder trafficLogExtractBuilder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficLogExtractInterceptor$Builder");
		Context trafficLogExtractCtx = new Context();
		trafficLogExtractBuilder.configure(trafficLogExtractCtx);
        Interceptor trafficLogExtractInterceptor = trafficLogExtractBuilder.build();
        event = trafficLogExtractInterceptor.intercept(event);
        
        Interceptor.Builder trafficHiveAddPartitionBuilder = InterceptorBuilderFactory.newInstance("com.nexr.traffic.flume.interceptor.TrafficHiveAddPartitionInterceptor$Builder");
		Context trafficHiveAddPartitionCtx = new Context();
		trafficHiveAddPartitionCtx.put("hdfs.path", "/user/ndap/repository/user/dominic/traffic/");
		trafficHiveAddPartitionCtx.put("hive.jdbcUrl", "jdbc:hive2://172.27.155.92:10000");
		trafficHiveAddPartitionCtx.put("hive.database", "dominic");
		trafficHiveAddPartitionCtx.put("hive.table", "hive_traffic");
		trafficHiveAddPartitionBuilder.configure(trafficHiveAddPartitionCtx);
        Interceptor trafficHiveAddPartitionInterceptor = trafficHiveAddPartitionBuilder.build();
        trafficHiveAddPartitionInterceptor.initialize();
        trafficHiveAddPartitionInterceptor.intercept(event);
	}

}
