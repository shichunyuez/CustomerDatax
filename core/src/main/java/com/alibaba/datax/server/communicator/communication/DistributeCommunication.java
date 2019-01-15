package com.alibaba.datax.server.communicator.communication;


import com.alibaba.datax.server.service.ZooKeeperService;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式的Communication
 */
public class DistributeCommunication extends Communication{

    private static final Logger LOG = LoggerFactory
            .getLogger(DistributeCommunication.class);

    private String zkPath=null;

    public DistributeCommunication(String zkPath)
    {
        this.zkPath=zkPath;

        if(this.zkPath!=null && !this.zkPath.trim().equals(""))
        {
            ZooKeeperService.registerDistributeCommunication(this);
        }
    }

    public String getZkPath() {
        return zkPath;
    }

    public void doZKCreated(String data)
    {
        LOG.info("add zk node: "+data);
    }

    public void doZKUpdated(String data)
    {
        LOG.info("update zk node: "+data);
        Communication communication=Communication.fromJson(JSONObject.parseObject(data));
        this.mergeFrom(communication);
    }

    public void doZKDeleted(String data)
    {
        LOG.info("delete zk node: "+data);
    }
}
