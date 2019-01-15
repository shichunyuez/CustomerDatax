package com.alibaba.datax.server.service;

import com.alibaba.datax.server.communicator.communication.DistributeCommunication;
import com.alibaba.datax.server.resource.NodeManager;
import com.alibaba.datax.server.scheduler.AbstractScheduler;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017/7/24 0024.
 */
public class ZooKeeperService implements Watcher {

    private static final Logger LOG = LoggerFactory
            .getLogger(ZooKeeperService.class);

    public final static String JOBSCHEDULER_ZKDIR="/datube/scheduler/job";
    public final static String TASKGROUPSCHEDULER_ZKDIR="/datube/scheduler/taskgroup";

    public final static String JOBCOMMUNICATION_ZKDIR="/datube/distcomm/job";
    public final static String TASKGROUPCOMMUNICATION_ZKDIR="/datube/distcomm/taskgroup";

    public final static String RESOURCE_ZKDIR="/datube/res";

    /*@Autowired
    private RequestScheduler requestScheduler;
    @Autowired
    private JobScheduler jobScheduler;
    @Autowired
    private TaskGroupScheduler taskGroupScheduler;
    @Autowired
    private ResourceManager resourceManager;*/


    //public final static String ROOT_ZKDIR_PATTERN="/datube";
    public final static String NODES_ZKDIR_PATTERN="/datube/nodes/*(!/)";///datube/nodes/[nodeID] , data是resource信息
    public final static String JOBS_ZKDIR_PATTERN="/datube/nodes/*(!/)/jobs/*(!/)";///datube/nodes/[nodeID]/jobs/[jobID], data是configuration
    public final static String JOB_COMMUNICATION_ZKDIR_PATTERN="/datube/nodes/*(!/)/jobs/*(!/)/communication";//data是job communication
    public final static String TASKGROUPS_ZKDIR_PATTERN="/datube/nodes/*(!/)/jobs/*(!/)/taskgroups/*(!/)";///datube/nodes/[nodeID]/jobs/[jobID]/taskgroups/[taskgroupID], data是configuration
    public final static String TASKGROUP_COMMUNICATION_ZKDIR_PATTERN="/datube/nodes/*(!/)/jobs/*(!/)/taskgroups/*(!/)/communication";//data是taskgroup communication

    //zookeeper地址
    private static String servers;
    private static int port=2186;
    //链接超时时间
    private static int sessionTimeout = 40000;

    private static ZooKeeper zk=null;

    private static Map<String,AbstractScheduler> schedulersMap=new HashMap();

    private static Map<String,DistributeCommunication> distributeCommunicationsMap=new HashMap();

    private static Map<String,NodeManager> resourceManagersMap=new HashMap();

    public static ZooKeeper getZookeeper()
    {
        if(zk==null)
        {
            try {
                zk=new ZooKeeper(servers+":"+port,sessionTimeout,new ZooKeeperService());
            }
            catch(Exception e)
            {
                LOG.error("连接Zookeeper服务器"+servers+"报错:",e);
            }
        }
        return zk;
    }

    public static void registerScheduler(AbstractScheduler scheduler)
    {
        //schedulersMap.put(scheduler.getZkPath(),scheduler);
    }

    public static void registerDistributeCommunication(DistributeCommunication distributeCommunication)
    {
        distributeCommunicationsMap.put(distributeCommunication.getZkPath(),distributeCommunication);
    }
    public static void registerResourceManager(NodeManager resourceManager)
    {
        //resourceManagersMap.put(resourceManager.getZkPath(),resourceManager);
    }

    private void processSchedulerEvent(WatchedEvent event)
    {
        //有新的work分配进来
        if(event.getType()==Event.EventType.NodeCreated)
        {
            AbstractScheduler scheduler=schedulersMap.get(event.getPath());
            if(scheduler==null)
            {
                LOG.error("无主的zookeeper path: "+event.getPath());
                return;
            }

            byte[] data=null;
            try
            {
                data=zk.getData(event.getPath(),false,null);
            }
            catch(Exception e)
            {
                LOG.error("获取数据出错zookeeper path: "+event.getPath(),e);
                return;
            }

            //scheduler.doZKCreated(new String(data));
        }
    }

    private void processDistributeCommunicationEvent(WatchedEvent event)
    {
        //distcomm分配进来
        if(event.getType()==Event.EventType.NodeCreated)
        {
            DistributeCommunication distributeCommunication=distributeCommunicationsMap.get(event.getPath());
            if(distributeCommunication==null)
            {
                LOG.error("无主的zookeeper path: "+event.getPath());
                return;
            }

            byte[] data=null;
            try
            {
                data=zk.getData(event.getPath(),false,null);
            }
            catch(Exception e)
            {
                LOG.error("获取数据出错zookeeper path: "+event.getPath(),e);
                return;
            }

            distributeCommunication.doZKCreated(new String(data));
        }
        //communication更新了
        else if(event.getType()==Event.EventType.NodeDataChanged)
        {
            DistributeCommunication distributeCommunication=distributeCommunicationsMap.get(event.getPath());
            if(distributeCommunication==null)
            {
                LOG.error("无主的zookeeper path: "+event.getPath());
                return;
            }

            byte[] data=null;
            try
            {
                data=zk.getData(event.getPath(),false,null);
            }
            catch(Exception e)
            {
                LOG.error("获取数据出错zookeeper path: "+event.getPath(),e);
                return;
            }

            distributeCommunication.doZKUpdated(new String(data));
        }
    }

    private void processResourceEvent(WatchedEvent event)
    {
        //有新的节点分配进来
        if(event.getType()==Event.EventType.NodeCreated)
        {
            NodeManager resourceManager=resourceManagersMap.get(event.getPath());
            if(resourceManager==null)
            {
                LOG.error("无主的zookeeper path: "+event.getPath());
                return;
            }

            byte[] data=null;
            try
            {
                data=zk.getData(event.getPath(),false,null);
            }
            catch(Exception e)
            {
                LOG.error("获取数据出错zookeeper path: "+event.getPath(),e);
                return;
            }

            //resourceManager.doZKCreated(new String(data));
        }
        //节点信息变化
        else if(event.getType()==Event.EventType.NodeDataChanged)
        {
            NodeManager resourceManager=resourceManagersMap.get(event.getPath());
            if(resourceManager==null)
            {
                LOG.error("无主的zookeeper path: "+event.getPath());
                return;
            }

            byte[] data=null;
            try
            {
                data=zk.getData(event.getPath(),false,null);
            }
            catch(Exception e)
            {
                LOG.error("获取数据出错zookeeper path: "+event.getPath(),e);
                return;
            }

            //resourceManager.doZKUpdated(new String(data));
        }
        //节点退出
        else if(event.getType()==Event.EventType.NodeDeleted)
        {
            NodeManager resourceManager=resourceManagersMap.get(event.getPath());
            if(resourceManager==null)
            {
                LOG.error("无主的zookeeper path: "+event.getPath());
                return;
            }

            byte[] data=null;
            try
            {
                data=zk.getData(event.getPath(),false,null);
            }
            catch(Exception e)
            {
                LOG.error("获取数据出错zookeeper path: "+event.getPath(),e);
                return;
            }

            //resourceManager.doZKDeleted(new String(data));
        }
    }

    @Override
    public void process(WatchedEvent event) {
        Pattern nodesPattern = Pattern.compile(NODES_ZKDIR_PATTERN);
        Pattern jobsPattern=Pattern.compile(JOBS_ZKDIR_PATTERN);
        Pattern jobComsPattern=Pattern.compile(JOB_COMMUNICATION_ZKDIR_PATTERN);
        Pattern taskGroupsPattern=Pattern.compile(TASKGROUPS_ZKDIR_PATTERN);
        Pattern taskGroupComsPattern=Pattern.compile(TASKGROUP_COMMUNICATION_ZKDIR_PATTERN);

        if(nodesPattern.matcher(event.getPath()).matches())
        {
            //创建节点
            if(event.getType()==Event.EventType.NodeCreated)
            {
                //resourceManager.newNode();
            }
            //更新节点
            else if(event.getType()==Event.EventType.NodeDataChanged)
            {
                //resourceManager.heartBeat();
            }
            //删除节点
            else if(event.getType()==Event.EventType.NodeDeleted)
            {
                //resourceManager.removeNode();
            }
        }
        else if(jobsPattern.matcher(event.getPath()).matches())
        {
            //收到job
            if(event.getType()==Event.EventType.NodeCreated)
            {
                //jobScheduler.syncSchedule();
            }
            //更新job configuration
            else if(event.getType()==Event.EventType.NodeDataChanged)
            {

            }
            //删除job
            else if(event.getType()==Event.EventType.NodeDeleted)
            {

            }
        }
        else if(jobComsPattern.matcher(event.getPath()).matches())
        {
            //创建job communication
            if(event.getType()==Event.EventType.NodeCreated)
            {

            }
            //更新job communication
            else if(event.getType()==Event.EventType.NodeDataChanged)
            {

            }
            //删除job communication
            else if(event.getType()==Event.EventType.NodeDeleted)
            {

            }
        }
        else if(taskGroupsPattern.matcher(event.getPath()).matches())
        {
            //创建taskgroup
            if(event.getType()==Event.EventType.NodeCreated)
            {
                //taskGroupScheduler.asyncSchedule();
            }
            //更新taskgroup
            else if(event.getType()==Event.EventType.NodeDataChanged)
            {

            }
            //删除taskgroup
            else if(event.getType()==Event.EventType.NodeDeleted)
            {

            }
        }
        else if(taskGroupComsPattern.matcher(event.getPath()).matches())
        {
            //创建taskgroup comminucation
            if(event.getType()==Event.EventType.NodeCreated)
            {

            }
            //更新taskgroup comminucation
            else if(event.getType()==Event.EventType.NodeDataChanged)
            {

            }
            //删除taskgroup comminucation
            else if(event.getType()==Event.EventType.NodeDeleted)
            {

            }
        }
        else
        {
            LOG.error("尚未支持的zookeeper path: "+event.getPath());
        }
    }
}
