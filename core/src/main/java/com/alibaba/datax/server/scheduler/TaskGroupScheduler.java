package com.alibaba.datax.server.scheduler;


import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.server.container.AbstractContainer;
import com.alibaba.datax.server.container.TaskContainer;
import com.alibaba.datax.server.container.TaskGroupContainer;
import com.alibaba.datax.server.work.AbstractWork;
import com.alibaba.datax.server.work.TaskGroup;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public class TaskGroupScheduler extends AbstractScheduler {

    private static final Logger LOG = LoggerFactory
            .getLogger(RequestScheduler.class);

    @Override
    public AbstractWork newWork(Configuration configuration) {
        return new TaskGroup(configuration);
    }

    @Override
    public AbstractContainer newSubWorksContainer(AbstractWork work,List<AbstractWork> subWorks) {
        return new TaskContainer((TaskGroup)work,subWorks);
    }

    @Override
    public String getWorksSourceZkpath() {
        return "/datube/nodes/*(!/)/taskgroups/*(!/)";
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
