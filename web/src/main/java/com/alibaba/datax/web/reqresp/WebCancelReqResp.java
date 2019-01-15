package com.alibaba.datax.web.reqresp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebCancelReqResp {
	private static final Logger LOG = LoggerFactory.getLogger(WebCancelReqResp.class);
	/*public WebResponse beWebCancel(MasterContext context, WebRequest req) {
		// 判断job是否在运行中，或者在等待队列
		// 如果在运行中，执行取消命令，如果在等待中，从等待队列删除
		// 如果不在，抛出异常，job没有在运行中
		if (req.getEk() == ExecuteKind.ScheduleKind) {
			return processScheduleCancel(context, req);
		} else if (req.getEk() == ExecuteKind.ManualKind) {
			return processManualCancel(context, req);
		} else if (req.getEk() == ExecuteKind.DebugKind) {
			return processDebugCancel(context, req);
		}
		return null;
	}*/

	/*private WebResponse processManualCancel(MasterContext context,
			WebRequest req) {
		WebResponse ret = null;
		String jobExecuteDetailID = req.getId();
		BDPMSJobExecuteDetail jobExecuteDetail = context.getJobExecuteDetailService().getByPk(Long.valueOf(jobExecuteDetailID));
		String logTxt="收到取消任务请求:"+jobExecuteDetail.getJobid().longValue()+",消息ID为:"+req.getRid();
		log.info("receive web cancel request,rid=" + req.getRid()
				+ ",jobExecuteDetailID=" + jobExecuteDetailID);
		//Long jobId = jobExecuteDetail.getJobid();
		for (MasterContext.QueueJob queueJob : new ArrayList<MasterContext.QueueJob>(context.getManualQueue())) {
			if (queueJob.getJobExeDetailID()==jobExecuteDetail.getId()) {
				if (context.getManualQueue().remove(queueJob)) {
					ret = WebResponse.newBuilder().setRid(req.getRid())
							.setOperate(req.getOperate()).setStatus(Status.OK)
							.build();
					jobExecuteDetail.setLogcontent(jobExecuteDetail.getLogcontent()+"\n任务被取消");
					logTxt="任务被取消: jobExecuteDetailID="+jobExecuteDetail.getId().longValue();
					context.getJobExecuteDetailService().end(jobExecuteDetail.getId(),logTxt,(long)GlobalConf.BDPMS_JOB_STATUS_CALCELED.getCode());
					break;
				}
			}
		}
		if (jobExecuteDetail.getTriggertypecode() == GlobalConf.BDPMS_JOB_TRIGGERTYPE_MANUAL.getCode()) {
			for (Channel key : new HashSet<Channel>(context.getWorkers()
					.keySet())) {
				MasterWorkerHolder worker = context.getWorkers().get(key);
				if (worker.getManualRunnings().containsKey(jobExecuteDetail.getId())) {
					Future<Response> f = new MasterCancelJobReqResp().cancel(context,
							worker.getChannel(), ExecuteKind.ManualKind,
							jobExecuteDetail.getId().longValue()+"");
					worker.getManualRunnings().remove(jobExecuteDetail.getId().longValue()+"");
					try {
						f.get(30, TimeUnit.SECONDS);
					} catch (Exception e) {
					}
					ret = WebResponse.newBuilder().setRid(req.getRid())
							.setOperate(req.getOperate()).setStatus(Status.OK)
							.build();
					log.info("send web cancel response,rid="
							+ req.getRid() + ",jobExecuteDetailID=" + jobExecuteDetail.getId().longValue());
				}
			}
		}

		if (ret == null) {
			// 找不到job，失败
			ret = WebResponse.newBuilder().setRid(req.getRid())
					.setOperate(req.getOperate()).setStatus(Status.ERROR)
					.setErrorText("Mannual任务中找不到匹配的job("+jobExecuteDetail.getJobid().longValue()+","+jobExecuteDetail.getId().longValue()+")，无法执行取消命令").build();
		}

		context.getJobExecuteDetailService().end(jobExecuteDetail.getId(),jobExecuteDetail.getLogcontent()+"\n任务取消.",Long.valueOf(GlobalConf.BDPMS_JOB_STATUS_CALCELED.getCode()));

		return ret;
	}*/

	/*private WebResponse processDebugCancel(MasterContext context, WebRequest req) {
		WebResponse ret = null;
		String debugId = req.getId();
		BDPMSDebugHisWithBLOBs history = context.getDebugHisService().getByPk(Long.valueOf(debugId));
		log.info("receive web debug cancel request,rid=" + req.getRid()
				+ ",debugId=" + debugId);
		for (MasterContext.QueueDebug debug : new ArrayList<MasterContext.QueueDebug>(context.getDebugQueue())) {
			if (debug.getDebugID()==Long.valueOf(debugId)) {
				if (context.getDebugQueue().remove(debugId)) {
					ret = WebResponse.newBuilder().setRid(req.getRid())
							.setOperate(req.getOperate()).setStatus(Status.OK)
							.build();
					history.setLogcontent(history.getLogcontent()+"\n任务被取消");
					context.getDebugHisService().updateLogContentByPk(history.getId(),history.getLogcontent());
					break;
				}
			}
		}
		for (Channel key : new HashSet<Channel>(context.getWorkers().keySet())) {
			MasterWorkerHolder worker = context.getWorkers().get(key);
			if (worker.getDebugRunnings().containsKey(debugId)) {
				Future<Response> f = new MasterCancelJobReqResp().cancel(context,
						worker.getChannel(), ExecuteKind.DebugKind, debugId);
				worker.getDebugRunnings().remove(debugId);
				try {
					f.get(10, TimeUnit.SECONDS);
				} catch (Exception e) {
				}
				ret = WebResponse.newBuilder().setRid(req.getRid())
						.setOperate(req.getOperate()).setStatus(Status.OK)
						.build();
				log.info("send web debug cancel response,rid="
						+ req.getRid() + ",debugId=" + debugId);
			}
		}
		if (ret == null) {
			// 找不到job，失败
			ret = WebResponse.newBuilder().setRid(req.getRid())
					.setOperate(req.getOperate()).setStatus(Status.ERROR)
					.setErrorText("Debug任务中找不到匹配的job("+history.getFileid().longValue()+","+history.getId().longValue()+")，无法执行取消命令").build();
		}

		context.getDebugHisService().end(history.getId(),null,Long.valueOf(GlobalConf.BDPMS_JOB_STATUS_CALCELED.getCode()));
		return ret;
	}*/

	/*private WebResponse processScheduleCancel(MasterContext context,
			WebRequest req) {
		WebResponse ret = null;
		String jobExeDetailId = req.getId();
		BDPMSJobExecuteDetail jobExecuteDetail = context.getJobExecuteDetailService().getByPk(Long.valueOf(jobExeDetailId));
		log.info("receive web cancel request,rid=" + req.getRid()
				+ ",jobExecuteDetailID=" + jobExeDetailId);
		//Long jobId = jobExecuteDetail.getJobid();
		for (MasterContext.QueueJob job : new ArrayList<MasterContext.QueueJob>(context.getQueue())) {
			if (job.getJobExeDetailID().longValue()==jobExecuteDetail.getId().longValue()) {
				if (context.getQueue().remove(job)) {
					ret = WebResponse.newBuilder().setRid(req.getRid())
							.setOperate(req.getOperate()).setStatus(Status.OK)
							.build();
					jobExecuteDetail.setLogcontent(jobExecuteDetail.getLogcontent()+"\n任务被取消");
					context.getJobExecuteDetailService().updateLogContentByPk(jobExecuteDetail.getId(),jobExecuteDetail.getLogcontent());
					break;
				}
			}
		}
		for (Channel key : new HashSet<Channel>(context.getWorkers().keySet())) {
			MasterWorkerHolder worker = context.getWorkers().get(key);
			if (worker.getRunnings().containsKey(jobExeDetailId)) {
				Future<Response> f = new MasterCancelJobReqResp().cancel(context,
						worker.getChannel(), ExecuteKind.ScheduleKind,
						jobExeDetailId);
				worker.getRunnings().remove(jobExeDetailId);
				try {
					f.get(10, TimeUnit.SECONDS);
				} catch (Exception e) {
				}
				ret = WebResponse.newBuilder().setRid(req.getRid())
						.setOperate(req.getOperate()).setStatus(Status.OK)
						.build();
				log.info("send web cancel response,rid=" + req.getRid()
						+ ",jobExeDetailID=" + jobExeDetailId);
			}
		}

		//老版本
		//if (ret == null) {
			// 数据库设置状态
			//JobStatus js = context.getGroupManager().getJobStatus(jobId);
			//js.setStatus(JobStatus.Status.WAIT);
			//js.setHistoryId(null);
			//context.getGroupManager().updateJobStatus(js);
			// 找不到job，失败
			//ret = WebResponse.newBuilder().setRid(req.getRid())
					//.setOperate(req.getOperate()).setStatus(Status.ERROR)
					//.setErrorText("Schedule任务中找不到匹配的job("+history.getJobId()+","+history.getId()+")，无法执行取消命令").build();
		//}

		context.getJobExecuteDetailService().end(jobExecuteDetail.getId(),jobExecuteDetail.getLogcontent()+"\n任务取消.",Long.valueOf(GlobalConf.BDPMS_JOB_STATUS_CALCELED.getCode()));
		return ret;
	}*/
}
