package com.alibaba.datax.server.work;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.server.communicator.communication.Communication;
import com.alibaba.datax.server.communicator.communication.DistributeCommunication;
import com.alibaba.datax.server.container.AbstractContainer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public class Request extends AbstractWork {

    public Request(Configuration configuration) {
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
        return null;
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
