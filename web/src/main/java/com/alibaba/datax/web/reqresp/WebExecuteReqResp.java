package com.alibaba.datax.web.reqresp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebExecuteReqResp {
	private static final Logger LOG = LoggerFactory.getLogger(WebExecuteReqResp.class);
	/*public WebResponse beWebExecute(MasterContext context,WebRequest req) {
		if(req.getEk()==ExecuteKind.ManualKind || req.getEk()==ExecuteKind.ScheduleKind){
			String jobExecuteDetailID=req.getId();
			BDPMSJobExecuteDetail jobExecuteDetail=context.getJobExecuteDetailService().getByPk(Long.valueOf(jobExecuteDetailID));
			context.getMasterSchedule().putIntoQueue(jobExecuteDetail);
			
			WebResponse resp=WebResponse.newBuilder().setRid(req.getRid()).setOperate(WebOperate.ExecuteJob)
				.setStatus(Status.OK).build();
			log.info("send web execute response,rid="+req.getRid()+",jobExecuteDetailID="+jobExecuteDetailID);
			return resp;
		}else if(req.getEk()==ExecuteKind.DebugKind){
			String debugId=req.getId();
			BDPMSDebugHisWithBLOBs debugHisWithBLOBs=context.getDebugHisService().getByPk(Long.valueOf(debugId));
			//DebugHistory history=context.getDebugHistoryManager().findDebugHistory(debugId);
			log.info("receive web debug request,rid="+req.getRid()+",debugId="+debugId);
			
			context.getMasterSchedule().putIntoDebugQueue(debugHisWithBLOBs);
			
			WebResponse resp=WebResponse.newBuilder().setRid(req.getRid()).setOperate(WebOperate.ExecuteJob)
				.setStatus(Status.OK).build();
			log.info("send web debug response,rid="+req.getRid()+",debugId="+debugId);
			return resp;
		}
		return null;
	}*/
}
