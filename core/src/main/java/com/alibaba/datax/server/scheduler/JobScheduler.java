package com.alibaba.datax.server.scheduler;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.server.container.AbstractContainer;
import com.alibaba.datax.server.container.TaskGroupContainer;
import com.alibaba.datax.server.work.AbstractWork;
import com.alibaba.datax.server.work.Job;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Administrator on 2017/7/26 0026.
 * JobScheduler负责监听ZK中是否有新分配的job,如果有则创建JobContainer开始执行
 */
public class JobScheduler extends AbstractScheduler {

    private static final Logger LOG = LoggerFactory
            .getLogger(JobScheduler.class);

    @Override
    public String getWorksSourceZkpath() {
        return "/datube/nodes/*(!/)/jobs/*(!/)";
    }

    @Override
    public AbstractWork newWork(Configuration configuration) {
        return new Job(configuration);
    }

    @Override
    public AbstractContainer newSubWorksContainer(AbstractWork work,List<AbstractWork> subWorks) {
        return new TaskGroupContainer((Job)work,subWorks);
    }

    @Override
    public void process(WatchedEvent event) {
        //if(nodesPattern.matcher(event.getPath()).matches())
        //{
        //新加入节点
        if(event.getType()==Event.EventType.NodeCreated)
        {
            //resourceManager.newNode();
            LOG.info("新分配work: "+event.getPath());
        }
        //删除节点
        else if(event.getType()==Event.EventType.NodeDeleted)
        {
            //resourceManager.removeNode();
            //LOG.info("节点退出: "+event.getPath());
        }
        //}
    }
}
