package com.alibaba.datax.server.container;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.JobPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.*;
import com.alibaba.datax.dataxservice.face.domain.enums.ExecuteMode;
import com.alibaba.datax.server.DefaultJobPluginCollector;
import com.alibaba.datax.server.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.server.communicator.JobContainerCommunicator;
import com.alibaba.datax.server.communicator.communication.Communication;
import com.alibaba.datax.server.util.*;
import com.alibaba.datax.server.util.ClassLoaderSwapper;
import com.alibaba.datax.server.util.CoreConstant;
import com.alibaba.datax.server.util.LoadUtil;
import com.alibaba.datax.server.work.AbstractWork;
import com.alibaba.datax.server.work.Job;
import com.alibaba.datax.server.work.Request;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public class JobContainer extends AbstractContainer {

    private static final Logger LOG = LoggerFactory
            .getLogger(JobContainer.class);

    @Override
    public AbstractContainerCommunicator initContainerCommunicator() {
        return new JobContainerCommunicator(this);
    }

    public JobContainer(Request request, List<AbstractWork> subWorks) {
        super(request,subWorks);
    }

    //@Override
    //public AbstractContainerCommunicator prepareCommunicator() {
        //return new JobContainerCommunicator();
    //}

    @Override
    public void logStat() {

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
