package com.alibaba.datax.server.work;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.server.communicator.communication.Communication;
import com.alibaba.datax.server.communicator.communication.DistributeCommunication;
import com.alibaba.datax.server.util.CoreConstant;

import java.util.List;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public class TaskGroup extends AbstractWork {

    public TaskGroup(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Communication initCommunication() {
        return new DistributeCommunication("");
    }

    @Override
    public boolean supportFailOver() {
        return false;
    }

    /*@Override
    public boolean isZKAssign() {
        return false;
    }*/

    @Override
    public void shutdown() {

    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public List<AbstractWork> split() {
        List<Configuration> taskConfigs = null;

        try {
            taskConfigs = this.getConfiguration().getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        }
        catch(Exception e)
        {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "无法获取到Task配置列表.");
        }

        this.getSubWorks().clear();
        for(Configuration taskConf : taskConfigs)
        {
            //this.getContainer().getCommunicator().register(taskConf.getInt(CoreConstant.TASK_ID));
            this.getSubWorks().add(new Task(taskConf));
        }

        return this.getSubWorks();
    }

    @Override
    public void addInit() throws Exception {

    }

    @Override
    public void addCheck() throws Exception {

    }

    @Override
    public void addPrepare() throws Exception {

    }

    @Override
    public void addSchedule() throws Exception {

    }

    @Override
    public void addPost() throws Exception {

    }

    @Override
    public void addDestroy() throws Exception {

    }
}
