package com.alibaba.datax.web.reqresp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateReqResp {
	private static final Logger LOG = LoggerFactory.getLogger(UpdateReqResp.class);
	/*public WebResponse beWebUpdate(MasterContext context,WebRequest req) {
		
		context.getDispatcher().forwardEvent(new JobMaintenanceEvent(Events.UpdateJob,Long.valueOf(req.getId())));
		WebResponse resp=WebResponse.newBuilder().setRid(req.getRid()).setOperate(WebOperate.UpdateJob)
			.setStatus(Status.OK).build();
		log.info("收到从Web端更新任务的请求,rid="+req.getRid()+",jobId="+req.getId());
		return resp;
	}*/
}
