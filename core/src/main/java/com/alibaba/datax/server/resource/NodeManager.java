package com.alibaba.datax.server.resource;

import com.alibaba.datax.server.service.ZookeeperSupport;
import com.alibaba.datax.server.work.AbstractWork;
import com.alibaba.fastjson.JSONObject;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Administrator on 2017/7/24 0024.
 */

public class NodeManager extends ZookeeperSupport implements Runnable{

    private static final Logger LOG = LoggerFactory
            .getLogger(NodeManager.class);

    private Map<String, Node> nodesMap=new HashMap<String, Node>();

    public static final String NODES_ZKPATH="/datube/nodes";

    private static NodeManager localNodeManager=null;

    static
    {
        localNodeManager=new NodeManager();
    }

    public static NodeManager getLocalNodeManager()
    {
        return localNodeManager;
    }

    private NodeManager()
    {

        listenChildren(NODES_ZKPATH);

        Node localNode=Node.getLocalNode();

        try {
            this.zk.create(NODES_ZKPATH+"/"+localNode.getNodeID(),localNode.toJson().toString().getBytes(),null, CreateMode.EPHEMERAL);
        }
        catch(Exception e)
        {
            LOG.error("创建ZK节点失败: ",e);
        }

        //开始向ZK汇报心跳
        new Thread(this).start();
    }

    public synchronized void assign(AbstractWork work)
    {
        //先按node的负载分数升序排序
        List<Map.Entry<String, Node>> list = new ArrayList<Map.Entry<String, Node>>(nodesMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Node>>() {
            @Override
            public int compare(Map.Entry<String, Node> o1, Map.Entry<String, Node> o2) {
                if(o2.getValue().getLoadRate()>o1.getValue().getLoadRate())
                {
                    return 1;
                }
                else if(o2.getValue().getLoadRate()==o1.getValue().getLoadRate())
                {
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
        });

        if(list.size()>0)
        {
            //第一个就是负载最低的
            String nodeZKPath=NODES_ZKPATH+"/"+list.get(0).getValue().getNodeID();

            //在ZK中创建work节点和communication节点,这时候work就有了ZKPath和CommunicationZKPath
            work.setZkPath(nodeZKPath+"/jobs");
            work.setCommunicationZKPath("/communication");
        }
        else
        {
            LOG.error("当前没有可以用于分配work的节点.");
        }
    }

    @Override
    public void childNodeCreated(String childID,String data)
    {
        LOG.info("新节点: "+childID);
        this.listenData(NODES_ZKPATH+"/"+childID);
        Node newNode=Node.fromJson(JSONObject.parseObject(data));
        this.nodesMap.put(newNode.getNodeID(),newNode);
    }

    @Override
    public void childNodeChanged(String childID,String data)
    {
        LOG.info("节点更新: "+childID);
        Node newNode=Node.fromJson(JSONObject.parseObject(data));
        this.nodesMap.put(newNode.getNodeID(),newNode);
    }

    @Override
    public void childNodeDeleted(String childID)
    {
        LOG.info("节点退出: "+childID);
    }

    @Override
    public void run() {
        while(true)
        {
            Node localNode=Node.getLocalNode();
            try {
                this.zk.setData(NODES_ZKPATH+"/"+localNode.getNodeID(),localNode.toJson().toString().getBytes(),0);
            }
            catch(Exception e)
            {
                LOG.error("更新ZK数据失败: ",e);
            }

            try
            {
                Thread.sleep(30000);
            }
            catch(Exception e)
            {

            }
        }

    }
}
