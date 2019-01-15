package com.alibaba.datax.server.communicator;

import com.alibaba.datax.server.communicator.communication.Communication;
import com.alibaba.datax.server.container.AbstractContainer;
import com.alibaba.datax.server.service.ZookeeperSupport;
import com.alibaba.datax.server.util.CommunicationTool;
import com.alibaba.datax.server.work.AbstractWork;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public abstract class AbstractContainerCommunicator extends ZookeeperSupport {

    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractContainerCommunicator.class);

    private Map<Integer, Communication> communicationMap =
            new ConcurrentHashMap<Integer, Communication>();

    //private AbstractCollector collector;
    //private AbstractReporter reporter;

    private AbstractContainer container=null;

    public AbstractContainerCommunicator(AbstractContainer container)
    {
        this.container=container;
    }

    //收集我的统计信息
    public Communication collect()
    {
        Communication communication=new Communication();
        for(Communication com : communicationMap.values())
        {
            communication.mergeFrom(com);
        }
        return communication;
    }

    //向上级汇报信息
    public Communication report(Communication lastContainerCommunication)
    {
        Communication nowContainerCommunication = this.collect();
        nowContainerCommunication.setTimestamp(System.currentTimeMillis());
        Communication reportCommunication = CommunicationTool.getReportCommunication(nowContainerCommunication,
                lastContainerCommunication, this.communicationMap.size());
        try
        {
            this.zk.setData(this.getContainer().getSourceWork().getCommunicationZKPath(),reportCommunication.toJson().toString().getBytes(),0);
        }
        catch(Exception e)
        {
            LOG.error("写入Zookeeper失败: ",e);
        }
        return reportCommunication;
    }

    public void register(AbstractWork work)
    {
        //listenPaths.add(work.getCommunicationZKPath());
        communicationMap.put(work.getWorkID(),work.getCommunication());
        //监控communication更新
        this.listenData(work.getCommunicationZKPath());
    }

    //public static Map<Integer, Communication> getCommunicationMap() {
        //return communicationMap;
    //}

    public Map<Integer, Communication> getFinishedCommunications()
    {
        Map<Integer, Communication> result=new ConcurrentHashMap<Integer, Communication>();

        for(Map.Entry<Integer, Communication> entry : communicationMap.entrySet()) {
            Integer taskId = entry.getKey();
            Communication taskCommunication = entry.getValue();
            if (taskCommunication.isFinished()) {
                result.put(taskId,taskCommunication);
            }
        }

        return result;
    }

    public void resetCommunication(Integer id){
        communicationMap.put(id, new Communication());
    }

    public AbstractContainer getContainer() {
        return container;
    }

    @Override
    public void childNodeCreated(String childID,String data)
    {

    }

    @Override
    public void childNodeChanged(String childID,String data)
    {
        LOG.info("收到Communication更新: "+childID+"    "+data);

    }

    @Override
    public void childNodeDeleted(String childID)
    {

    }
}
