package com.alibaba.datax.server.container;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.server.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.server.communicator.TaskGroupContainerCommunicator;
import com.alibaba.datax.server.util.CoreConstant;
import com.alibaba.datax.server.work.AbstractWork;
import com.alibaba.datax.server.work.Job;
import com.alibaba.datax.server.work.TaskGroup;

import java.util.List;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public class TaskGroupContainer extends AbstractContainer {

    private String channelClazz=null;

    private String taskCollectorClass=null;

    @Override
    public AbstractContainerCommunicator initContainerCommunicator() {
        return new TaskGroupContainerCommunicator(this);
    }

    public TaskGroupContainer(Job job, List<AbstractWork> subWorks) {
        super(job,subWorks);
    }

    @Override
    public void addCheck() throws Exception{

    }

    @Override
    public void addInit() throws Exception{
        //this.channelClazz = this.getConfiguration().getString(
                //CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);
        //this.taskCollectorClass = this.getConfiguration().getString(
                //CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);

        /*List<Configuration> taskConfigs = null;

        try {
            taskConfigs = this.getConfiguration().getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        }
        catch(Exception e)
        {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "无法获取到Task配置列表.");
        }

        for(Configuration taskConf : taskConfigs)
        {
            this.getCommunicator().register(taskConf.getInt(CoreConstant.TASK_ID));
        }*/
    }

    @Override
    public void addPrepare() throws Exception{

    }

    @Override
    public void addSchedule() throws Exception {

    }

    @Override
    public void addPost() throws Exception{

    }

    @Override
    public void addDestroy() throws Exception{

    }

    @Override
    public void logStat() {

    }
}
