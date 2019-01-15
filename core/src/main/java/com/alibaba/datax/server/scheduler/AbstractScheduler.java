package com.alibaba.datax.server.scheduler;


import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.server.container.AbstractContainer;
import com.alibaba.datax.server.service.ZooKeeperService;
import com.alibaba.datax.server.service.ZookeeperSupport;
import com.alibaba.datax.server.work.AbstractWork;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public abstract class AbstractScheduler extends ZookeeperSupport {

    //public static final String ZOOKEEPER_PATH="/datube/nodes";
    //public final static String JOBS_ZKDIR_PATTERN="/datube/nodes/*(!/)/jobs/*(!/)";
    //public final static String TASKGROUPS_ZKDIR_PATTERN="/datube/nodes/*(!/)/jobs/*(!/)/taskgroups/*(!/)";

    //work来源，用于监听是否有work被分配过来
    //public final static String WORKS_SOURCE_ZKPATH="";
    //调度对象，用于写入和监听,写入是分配work,监听是收集反馈信息
    //public final static String SUBWORKS_ZKPATH="";
    //public final static String SUBWORKS_COMM_ZKPATH="";

    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractScheduler.class);

    private Queue<AbstractWork> runningWorks=new ConcurrentLinkedQueue<AbstractWork>();

    public abstract String getWorksSourceZkpath();

    public AbstractScheduler()
    {
        listenChildren(getWorksSourceZkpath());
    }

    public Integer asyncSchedule(Configuration configuration)
    {
        AbstractWork work=newWork(configuration);
        runningWorks.add(work);
        List<AbstractWork> subWorks=work.split();
        AbstractContainer subWorkContainer=newSubWorksContainer(work,subWorks);
        subWorkContainer.start();

        return work.getWorkID();
    }

    public void syncSchedule(Configuration configuration)
    {
        AbstractWork work=newWork(configuration);
        //设置新收到的work的一些已知属性
        work.setZkPath("");
        work.setCommunicationZKPath("");
        work.setWorkIDForZKPath("");//父work的WorkIDForZKPath拼接上work的workID
        work.setWorkID(1);

        runningWorks.add(work);
        List<AbstractWork> subWorks=work.split();
        AbstractContainer subWorkContainer=newSubWorksContainer(work,subWorks);
        subWorkContainer.start();
    }

    public abstract AbstractWork newWork(Configuration configuration);

    public abstract AbstractContainer newSubWorksContainer(AbstractWork work,List<AbstractWork> subWorks);

    @Override
    public void childNodeCreated(String childID,String data)
    {
        LOG.info("新分配的 workID: "+childID+"    "+data);
    }

    @Override
    public void childNodeChanged(String childID,String data)
    {

    }

    @Override
    public void childNodeDeleted(String childID)
    {

    }
}
