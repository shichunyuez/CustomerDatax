package com.alibaba.datax.web.reqresp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebDebugReqResp {
	private static final Logger LOG = LoggerFactory.getLogger(WebDebugReqResp.class);
	/*public WebResponse beWebExecute(MasterContext context,WebRequest req) {
		// 判断job是否已经在运行中，或者在队列中
		// 如果在，抛出异常，已经在执行中
		// 如果不在，将该job放入等待队列
		log.info("receive web debug request,rid="+req.getRid()+",debugId="+req.getId());
		String debugId=req.getId();
		for(MasterContext.QueueDebug debug:new ArrayList<MasterContext.QueueDebug>(context.getDebugQueue())){
			if(debug.getDebugID()==Long.valueOf(debugId)){
				WebResponse resp=WebResponse.newBuilder().setRid(req.getRid()).setOperate(WebOperate.ExecuteDebug)
					.setStatus(Status.ERROR).setErrorText("已经在队列中，无法再次运行").build();
				return resp;
			}
		}
		BDPMSDebugHisWithBLOBs debugHisWithBLOBs=context.getDebugHisService().getByPk(Long.valueOf(debugId));
		context.getMasterSchedule().putIntoDebugQueue(debugHisWithBLOBs);
		
		WebResponse resp=WebResponse.newBuilder().setRid(req.getRid()).setOperate(WebOperate.ExecuteDebug)
			.setStatus(Status.OK).build();
		log.info("send web debug response,rid="+req.getRid()+",debugId="+debugId);
		return resp;
	}*/
}
