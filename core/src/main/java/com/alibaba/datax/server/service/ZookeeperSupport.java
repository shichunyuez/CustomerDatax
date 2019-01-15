package com.alibaba.datax.server.service;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Administrator on 2017/8/16 0016.
 */
public abstract class ZookeeperSupport implements Watcher{

    private static final Logger LOG = LoggerFactory
            .getLogger(ZookeeperSupport.class);

    protected ZooKeeper zk=null;

    //public abstract List<String> getListenPaths();

    private Set<String> listeningChildrenPaths=new HashSet<String>();

    private Set<String> listeningDataPaths=new HashSet<String>();

    private void init()
    {
        try {
            zk=new ZooKeeper("",400000,null);
        }
        catch(Exception e)
        {
            LOG.error("Zookeeper连接失败: ",e);
        }
    }

    /*protected void listenZK()
    {
        List<String> listenerPaths=getListenPaths();
        for(String listenerPath : listenerPaths)
        {
            if(listenerPath!=null && !listenerPath.trim().equals(""))
            {
                try
                {
                    this.zk.getChildren(listenerPath,this);
                }
                catch(Exception e)
                {
                    LOG.error("Zookeeper 监听 "+listenerPath+" 失败: ",e);
                }
            }
        }
    }*/

    public void listenChildren(String path)
    {
        listeningChildrenPaths.add(path);
        try
        {
            this.zk.getChildren(path,this);
        }
        catch(Exception e)
        {
            LOG.error("Zookeeper 监听 "+path+" 失败: ",e);
        }
    }

    public void listenData(String path)
    {
        listeningDataPaths.add(path);
        try
        {
            this.zk.getData(path,this,null);
        }
        catch(Exception e)
        {
            LOG.error("Zookeeper 监听 "+path+" 失败: ",e);
        }
    }

    public ZookeeperSupport()
    {
        init();
    }

    public abstract void childNodeCreated(String childID,String data);

    public abstract void childNodeChanged(String childID,String data);

    public abstract void childNodeDeleted(String childID);

    private String getChildID(String path)
    {
        if(path!=null && !path.trim().equals(""))
        {
            String pathTmp=path;
            if(pathTmp.endsWith("/"))
            {
                pathTmp=pathTmp.substring(0,pathTmp.length()-1);
            }
            int idx=pathTmp.lastIndexOf("/");

            return pathTmp.substring(idx,pathTmp.length());

        }

        return null;
    }

    @Override
    public void process(WatchedEvent event) {
        //新加入节点
        if(event.getType()==Event.EventType.NodeCreated)
        {
            byte[] data;
            try {
                data=this.zk.getData(event.getPath(),false,null);
            }
            catch (Exception e)
            {
                LOG.error("Zookeeper 获取数据失败: ",e);
                return;
            }

            childNodeCreated(getChildID(event.getPath()),new String(data));
        }
        //删除节点
        else if(event.getType()==Event.EventType.NodeDeleted)
        {
            //resourceManager.removeNode();
            //LOG.info("节点退出: "+event.getPath());
            childNodeDeleted(getChildID(event.getPath()));
        }
        else if(event.getType()==Event.EventType.NodeDataChanged)
        {
            byte[] data;
            try {
                data=this.zk.getData(event.getPath(),false,null);
            }
            catch (Exception e)
            {
                LOG.error("Zookeeper 获取数据失败: ",e);
                return;
            }

            childNodeChanged(getChildID(event.getPath()),new String(data));
        }
        else
        {
            LOG.info("未知事件: "+event.getPath());
        }
    }
}
