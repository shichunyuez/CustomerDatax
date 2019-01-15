package com.alibaba.datax.server.work;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.server.App;
import com.alibaba.datax.server.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.server.communicator.communication.Communication;
import com.alibaba.datax.server.container.AbstractContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public abstract class AbstractWork implements App {

    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractWork.class);

    private int workID;
    private String workIDForZKPath=null;
    protected List<AbstractWork> subWorks=new ArrayList<AbstractWork>();
    private AbstractContainer container=null;
    private Configuration configuration=null;

    private String zkPath=null;
    private String communicationZKPath=null;

    private Communication communication;

    private int attemptCount;

    //public abstract void init(AbstractContainerCommunicator containerCommunicator);

    public abstract List<AbstractWork> split();

    //private int generateWorkID()
    //{
        //return 0;
    //}

    public String getZkPath()
    {
        if(this.zkPath==null)
        {
            LOG.error("zkPath还没有被设置,只有work被分配后才会被设置这个属性.");
        }
        return this.zkPath;
    }

    public void setZkPath(String zkPath) {
        this.zkPath = zkPath;
    }

    public String getCommunicationZKPath()
    {
        if(this.zkPath==null)
        {
            LOG.error("communicationZKPath还没有被设置,只有work被分配后才会被设置这个属性.");
        }
        return this.communicationZKPath;
    }

    public void setCommunicationZKPath(String communicationZKPath) {
        this.communicationZKPath = communicationZKPath;
    }

    public AbstractWork(Configuration configuration) {
        this.configuration = configuration;
        this.communication=initCommunication();

        //this.workID=generateWorkID();
    }

    public abstract Communication initCommunication();

    public int getWorkID() {
        return workID;
    }

    public void setWorkID(int workID) {
        this.workID = workID;
    }

    public String getWorkIDForZKPath() {
        if(this.workIDForZKPath==null)
        {
            LOG.error("WorkIDForZKPath还没有被设置,只有work被放入了container里才会被设置这个属性.");
        }
        return workIDForZKPath;
    }

    public void setWorkIDForZKPath(String workIDForZKPath) {
        this.workIDForZKPath = workIDForZKPath;
    }

    public Configuration getConfiguration()
    {
        return this.configuration;
    }

    public List<AbstractWork> getSubWorks() {
        return subWorks;
    }

    public void putIntoContainer(AbstractContainer container) {
        this.container = container;
    }

    public AbstractContainer getContainer() {
        if(this.container==null)
        {
            LOG.error("container还没有被设置,只有work被放入了container里才会被设置这个属性.");
        }
        return container;
    }

    public Communication getCommunication() {
        return communication;
    }

    public int getAttemptCount(){
        return attemptCount;
    }

    public abstract boolean supportFailOver();

    //public abstract boolean isZKAssign();

    private final void init() throws Exception
    {
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

    private final void schedule() throws Exception
    {
        addSchedule();
    }

    //执行入口
    public final void start()
    {
        LOG.info("Work start...");

        try
        {
            this.init();
        }
        catch(Exception e)
        {
            LOG.error("Work初始化失败:",e);
        }

        try {
            this.check();
        }
        catch(Exception e)
        {
            LOG.error("Work前期检查失败:",e);
        }

        try
        {
            this.prepare();
        }
        catch(Exception e)
        {
            LOG.error("Work准备失败:",e);
        }

        try
        {
            this.schedule();
        }
        catch(Exception e)
        {
            LOG.error("Work执行失败:",e);
        }

        try
        {
            this.post();
        }
        catch(Exception e)
        {
            LOG.error("Work post失败:",e);
        }

        try
        {
            this.destroy();
        }
        catch(Exception e)
        {
            LOG.error("Work destroy失败:",e);
        }
    }

    public abstract void shutdown();

    public abstract boolean isShutdown();

    public abstract boolean isFinished();

    public void reset()
    {
        this.communication=new Communication();
    }
}
