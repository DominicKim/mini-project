package com.nexr.traffic.flume.serializer;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.traffic.flume.common.TrafficConstants;

public class TrafficHbaseOverspeedEventSerializer implements HbaseEventSerializer {

	private static final Logger logger = LoggerFactory.getLogger(TrafficHbaseOverspeedEventSerializer.class);
	
	private static final byte[] QUALIFIER_HOUR = Bytes.toBytes("0");
	private static final byte[] QUALIFIER_MIN = Bytes.toBytes("1");
	private static final byte[] QUALIFIER_ROAD = Bytes.toBytes("2");
	private static final byte[] QUALIFIER_CAM = Bytes.toBytes("3");
	private static final byte[] QUALIFIER_SPEED = Bytes.toBytes("4");
	
	private static final String OVERSPEED = "overspeed";
	private static final Integer DEFAILT_OVERSPEED = 80;
	
	private String date;
	private String hour;
	private String body;
	private byte[] cf;
	private int overspeed;

	@Override
	public void configure(Context context) {
	    this.overspeed = context.getInteger(OVERSPEED, DEFAILT_OVERSPEED).intValue();
	    logger.debug("overspeed:" + this.overspeed);
	}

	@Override
	public void configure(ComponentConfiguration conf) {
		// no-op
	}
	
	@Override
	public void initialize(Event event, byte[] columnFamily) {
		if (event != null) {
			this.date = ((String) event.getHeaders().get(TrafficConstants.HEADER_DATE));
			this.hour = ((String) event.getHeaders().get(TrafficConstants.HEADER_HOUR));
			this.body = new String(event.getBody());
		} else {
			logger.error("Event is null");
		}
		if (columnFamily != null)
			this.cf = columnFamily;
		else
			logger.error("columnFamily is null");
	}

	@Override
	public List<Row> getActions() {
	    List<Row> rows = new ArrayList<Row>();

		if (this.body != null) {
			String[] log = this.body.split(",");
			String min = log[0];
			String road = log[1];
			String cam = log[2];
			String[] carInfos = log[3].split("\\|");

			for (String carInfo : carInfos) {
				String[] carNumCarSpeed = carInfo.split("\\^");
				String carNum = carNumCarSpeed[0];
				String carspeed = carNumCarSpeed[1];

				if (Integer.parseInt(carspeed) > this.overspeed) {
					String rowKey = this.date + "|" + carNum;
					Put put = new Put(Bytes.toBytes(rowKey));
					put.add(this.cf, QUALIFIER_HOUR, Bytes.toBytes(this.hour));
					put.add(this.cf, QUALIFIER_MIN, Bytes.toBytes(min));
					put.add(this.cf, QUALIFIER_ROAD, Bytes.toBytes(road));
					put.add(this.cf, QUALIFIER_CAM, Bytes.toBytes(cam));
					put.add(this.cf, QUALIFIER_SPEED, Bytes.toBytes(carspeed));
					
					rows.add(put);
				}
			}

		}

		return rows;
	}

	@Override
	public List<Increment> getIncrements() {
		return new ArrayList<Increment>();
	}
	
	@Override
	public void close() {
		// no-op
	}

}
