package com.alibaba.datax.web.reqresp;

import com.alibaba.datax.web.MasterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartBeatReqResp {
	private static final Logger LOG = LoggerFactory.getLogger(HeartBeatReqResp.class);
	private MasterContext context=null;
	/*public void beHeartBeat(MasterContext context,Channel channel, Protocol.Request request) {
		LOG.info("收到心跳: "+channel.getRemoteAddress());
		MasterWorkerHolder worker=context.getWorkers().get(channel);
		HeartBeatInfo newbeat=worker.new HeartBeatInfo();
		Protocol.HeartBeatMessage hbm;
		try {
			hbm = Protocol.HeartBeatMessage.newBuilder().mergeFrom(request.getBody()).build();
			newbeat.memRate=hbm.getMemRate();
			newbeat.runnings=hbm.getRunningsList();
			newbeat.debugRunnings=hbm.getDebugRunningsList();
			newbeat.manualRunnings=hbm.getManualRunningsList();
			newbeat.timestamp=new Date(hbm.getTimestamp());
			if(worker.heart==null || newbeat.timestamp.getTime()>worker.heart.timestamp.getTime()){
				worker.heart=newbeat;
			}
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
	}*/
}
