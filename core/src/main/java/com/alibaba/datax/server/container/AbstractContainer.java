package com.alibaba.datax.server.container;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.datax.server.App;
import com.alibaba.datax.server.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.server.communicator.communication.Communication;
import com.alibaba.datax.server.resource.NodeManager;
import com.alibaba.datax.server.util.FrameworkErrorCode;
import com.alibaba.datax.server.work.AbstractWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Administrator on 2017/7/26 0026.
 * Work的容器,work执行期间进行资源管理
 */
public abstract class AbstractContainer implements App{

    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractContainer.class);

    private AbstractWork sourceWork;
    //private AbstractContainerCommunicator communicator=null;
    private List<AbstractWork> subWorks=new ArrayList<AbstractWork>();

    //private AbstractWork parentWork=null;

    private Map<Integer, AbstractWork> workMap=null;
    private List<AbstractWork> workQueueWaiting=null;
    private Map<Integer, AbstractWork> workFailedMap=null;
    private List<AbstractWork> runingWorks=null;
    private Map<Integer, Long> workStartTimeMap=null;

    private int sleepIntervalInMillSec=100;
    private long reportIntervalInMillSec=10000l;
    private int workMaxRetryTimes=1;
    private int maxRunningWorks=5;
    private long workRetryIntervalInMsec=10000l;
    private long workMaxWaitInMsec=60000l;

    private long startTimeStamp;

    private long endTimeStamp;

    //private AbstractCollector collector=null;
    //private AbstractReporter reporter=null;

    private AbstractContainerCommunicator containerCommunicator=null;

    //private Communication containerCommunication;

    public abstract AbstractContainerCommunicator initContainerCommunicator();

    public AbstractContainer(AbstractWork sourceWork,List<AbstractWork> subWorks)
    {
        this.containerCommunicator=initContainerCommunicator();
        this.sourceWork=sourceWork;

        for(int i=0; i<subWorks.size();i++)
        {
            AbstractWork subWork=subWorks.get(i);
            this.subWorks.add(subWork);
            //加入了container之后,work便有了ID和IDForZKPath,并且被设置了container
            subWork.setWorkID(i);
            subWork.setWorkIDForZKPath(sourceWork.getWorkIDForZKPath()+"_"+subWork.getWorkID());
            subWork.putIntoContainer(this);
        }
    }

    //public AbstractContainerCommunicator getCommunicator() {
        //return communicator;
    //}

    //public abstract AbstractContainerCommunicator prepareCommunicator();

    /*protected void schedule()
    {
        //先进行分配
        List<Resource> resources=resourceManager.getCorrectResources(works.size());
        for(int i=0; i<resources.size(); i++)
        {
            resources.get(i).assign(works.get(i));
        }
    }*/

    private AbstractWork removeWork(List<AbstractWork> workList, int workId){
        Iterator<AbstractWork> iterator = workList.iterator();
        while(iterator.hasNext()){
            AbstractWork work = iterator.next();
            if(work.getWorkID() == workId){
                iterator.remove();
                return work;
            }
        }
        return null;
    }

    private boolean isAllTaskDone(){
        for(AbstractWork work : runingWorks){
            if(!work.isFinished()){
                return false;
            }
        }
        return true;
    }

    protected void schedule()
    {
        long lastReportTimeStamp = 0;
        Communication lastTaskGroupContainerCommunication = new Communication();

        try
        {
            while (true) {
                //1.判断task状态
                boolean failedOrKilled = false;
                //处理结束的任务,执行失败，被杀死，执行成功都算是结束的任务
                for(AbstractWork work : this.runingWorks)
                {
                    if(!work.getCommunication().isFinished())
                    {
                        continue;
                    }

                    AbstractWork workFinished = removeWork(this.runingWorks, work.getWorkID());

                    //上面从runTasks里移除了，因此对应在monitor里移除
                    //taskMonitor.removeTask(taskId);

                    //失败，看work是否支持failover，重试次数未超过最大限制
                    if(work.getCommunication().getState() == State.FAILED){
                        workFailedMap.put(work.getWorkID(), workFinished);
                        if(workFinished.supportFailOver() && workFinished.getAttemptCount() < workMaxRetryTimes){
                            workFinished.shutdown(); //关闭老的executor
                            work.reset(); //将work的状态重置
                            AbstractWork workRetry = workMap.get(work.getWorkID());
                            workQueueWaiting.add(workRetry); //重新加入任务列表
                        }else{
                            failedOrKilled = true;
                            break;
                        }
                    }else if(work.getCommunication().getState() == State.KILLED){
                        failedOrKilled = true;
                        break;
                    }else if(work.getCommunication().getState() == State.SUCCEEDED){
                        Long taskStartTime = workStartTimeMap.get(work.getWorkID());
                        if(taskStartTime != null){
                            Long usedTime = System.currentTimeMillis() - taskStartTime;
                            //LOG.info("taskGroup[{}] workId[{}] is successed, used[{}]ms",
                                    //this.parentWork.getWorkID(), work.getWorkID(), usedTime);
                            LOG.info("workId[{}] is successed, used[{}]ms",
                                    work.getWorkID(), usedTime);
                            //usedTime*1000*1000 转换成PerfRecord记录的ns，这里主要是简单登记，进行最长任务的打印。因此增加特定静态方法
                            //PerfRecord.addPerfRecord(this.parentWork.getWorkID(), work.getWorkID(), PerfRecord.PHASE.TASK_TOTAL,taskStartTime, usedTime * 1000L * 1000L);
                            workStartTimeMap.remove(work.getWorkID());
                            workMap.remove(work.getWorkID());
                        }
                    }
                }

                // 2.发现该container下work的总状态失败则汇报错误
                if (failedOrKilled) {
                    lastTaskGroupContainerCommunication = this.containerCommunicator.report(lastTaskGroupContainerCommunication);

                    throw DataXException.asDataXException(
                            FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, lastTaskGroupContainerCommunication.getThrowable());
                }

                //3.有任务未执行，且正在运行的任务数小于最大通道限制
                Iterator<AbstractWork> iterator = workQueueWaiting.iterator();
                while(iterator.hasNext() && runingWorks.size() < maxRunningWorks){
                    AbstractWork workWaiting = iterator.next();
                    Integer workId = workWaiting.getWorkID();
                    int attemptCount = 1;
                    AbstractWork lastWork = workFailedMap.get(workId);
                    //如果是失败重试
                    if(lastWork!=null){
                        attemptCount = lastWork.getAttemptCount() + 1;
                        long now = System.currentTimeMillis();
                        long failedTime = lastWork.getCommunication().getTimestamp();
                        if(now - failedTime < workRetryIntervalInMsec){  //未到等待时间，继续留在队列
                            continue;
                        }
                        if(!lastWork.isShutdown()){ //上次失败的task仍未结束
                            if(now - failedTime > workMaxWaitInMsec){
                                //Communication communication = containerCommunicator.getCommunication(workId);
                                //communication.setState(State.FAILED);
                                workWaiting.getCommunication().setState(State.FAILED);
                                this.containerCommunicator.report(lastTaskGroupContainerCommunication);
                                throw DataXException.asDataXException(CommonErrorCode.WAIT_TIME_EXCEED, "work failover等待超时");
                            }else{
                                lastWork.shutdown(); //再次尝试关闭
                                continue;
                            }
                        }else{
                            //LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] has already shutdown",
                                    //this.parentWork.getWorkID(), workId, lastWork.getAttemptCount());
                            LOG.info("workId[{}] attemptCount[{}] has already shutdown",
                                    workId, lastWork.getAttemptCount());
                        }
                    }
                    //Configuration workConfigForRun = taskMaxRetryTimes > 1 ? taskConfig.clone() : taskConfig;
                    //AbstractWork workForRun = new AbstractWork(workConfigForRun);
                    workWaiting.reset();
                    AbstractWork workForRun =workWaiting;
                    workStartTimeMap.put(workId, System.currentTimeMillis());
                    workForRun.start();

                    iterator.remove();
                    runingWorks.add(workForRun);

                    //上面，增加task到runTasks列表，因此在monitor里注册。
                    //taskMonitor.registerTask(workId, lastWork.getCommunication());

                    workFailedMap.remove(workId);
                    //LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] is started",
                            //this.parentWork.getWorkID(), workId, attemptCount);
                    LOG.info("taskId[{}] attemptCount[{}] is started",
                            workId, attemptCount);
                }

                //4.任务列表为空，executor已结束, 搜集状态为success--->成功
                if (workQueueWaiting.isEmpty() && isAllTaskDone() && this.containerCommunicator.collect().getState() == State.SUCCEEDED) {
                    // 成功的情况下，也需要汇报一次。否则在任务结束非常快的情况下，采集的信息将会不准确
                    lastTaskGroupContainerCommunication = this.containerCommunicator.report(lastTaskGroupContainerCommunication);

                    //LOG.info("taskGroup[{}] completed it's tasks.", this.parentWork.getWorkID());
                    LOG.info("Works Completed.");
                    break;
                }

                // 5.如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                    lastTaskGroupContainerCommunication = this.containerCommunicator.report(lastTaskGroupContainerCommunication);

                    lastReportTimeStamp = now;

                    //taskMonitor对于正在运行的task，每reportIntervalInMillSec进行检查
                    for(AbstractWork work:runingWorks){
                        //taskMonitor.report(work.getWorkID(),work.getCommunication());
                    }

                }

                Thread.sleep(sleepIntervalInMillSec);
            }
        }
        catch(Throwable e)
        {
            Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
            if (nowTaskGroupContainerCommunication.getThrowable() == null) {
                nowTaskGroupContainerCommunication.setThrowable(e);
            }
            nowTaskGroupContainerCommunication.setState(State.FAILED);
            this.containerCommunicator.report(nowTaskGroupContainerCommunication);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }finally {
            if(!PerfTrace.getInstance().isJob()){
                //最后打印cpu的平均消耗，GC的统计
                VMInfo vmInfo = VMInfo.getVmInfo();
                if (vmInfo != null) {
                    vmInfo.getDelta(false);
                    LOG.info(vmInfo.totalString());
                }

                LOG.info(PerfTrace.getInstance().summarizeNoException());
            }
        }
    }

    public abstract void logStat();

    private final void init() throws Exception
    {
        this.workMap = new HashMap<Integer, AbstractWork>(); //workId与work配置
        this.workQueueWaiting = new LinkedList<AbstractWork>(); //待运行work列表
        this.workFailedMap = new HashMap<Integer, AbstractWork>(); //workId与上次失败实例
        this.runingWorks = new ArrayList<AbstractWork>(this.subWorks.size()); //正在运行work
        this.workStartTimeMap = new HashMap<Integer, Long>(); //任务开始时间

        //this.communicator=prepareCommunicator();
        for(AbstractWork work : this.subWorks)
        {
            //work.setContainer(this);
            this.workMap.put(work.getWorkID(),work);
            this.workQueueWaiting.add(work);
        }

        addInit();
    }

    private final void check() throws Exception
    {

        addCheck();
    }

    private final void prepare() throws Exception
    {

        addPrepare();
    }

    private final void post() throws Exception
    {

        addPost();
    }

    private final void destroy() throws Exception
    {

        addDestroy();
    }

    public final void start()
    {
        LOG.info("Container start...");
        //this.getWork().start();

        this.startTimeStamp = System.currentTimeMillis();

        try
        {
            this.init();
        }
        catch(Exception e)
        {
            LOG.error("container初始化失败:",e);
        }

        try {
            this.check();
        }
        catch(Exception e)
        {
            LOG.error("container前期检查失败:",e);
        }

        try
        {
            this.prepare();
        }
        catch(Exception e)
        {
            LOG.error("container准备失败:",e);
        }

        //this.split();
        this.schedule();
        //this.getWork().schedule();

        try
        {
            this.post();
        }
        catch(Exception e)
        {
            LOG.error("container post失败:",e);
        }

        try
        {
            this.destroy();
        }
        catch(Exception e)
        {
            LOG.error("caontainer destroy失败:",e);
        }

        this.logStat();
    }

    public AbstractWork getSourceWork() {
        return sourceWork;
    }
}
