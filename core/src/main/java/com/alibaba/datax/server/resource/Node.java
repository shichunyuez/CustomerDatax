package com.alibaba.datax.server.resource;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * Created by Administrator on 2017/7/25 0025.
 */
public class Node {

    private static final Logger LOG = LoggerFactory
            .getLogger(Node.class);

    public static String LOCAL_IP=null;

    private double cpuUsedRate;
    private double memUsedRate;
    private double diskIORate;
    private double netIORate;
    private String ip;
    private String nodeID;

    static
    {
        try {
            LOCAL_IP= InetAddress.getLocalHost().getHostAddress();
        }
        catch(Exception e)
        {
            LOG.error("无法获取主机IP: ",e);
        }
    }

    public double getCpuUsedRate() {
        return cpuUsedRate;
    }

    public void setCpuUsedRate(double cpuUsedRate) {
        this.cpuUsedRate = cpuUsedRate;
    }

    public double getMemUsedRate() {
        return memUsedRate;
    }

    public void setMemUsedRate(double memUsedRate) {
        this.memUsedRate = memUsedRate;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public JSONObject toJson()
    {
        JSONObject result=new JSONObject();
        result.put("memUsedRate",memUsedRate);
        result.put("cpuUsedRate",cpuUsedRate);
        result.put("ip",ip);
        result.put("nodeID",nodeID);

        return result;
    }

    public static Node fromJson(JSONObject obj)
    {
        Node res=new Node();
        res.setMemUsedRate(obj.getDouble("memUsedRate"));
        res.setCpuUsedRate(obj.getDouble("cpuUsedRate"));
        res.setIp(obj.getString("ip"));
        res.setNodeID(obj.getString("nodeID"));

        return res;
    }

    public static Node getLocalNode()
    {
        Node node=new Node();
        return node;
    }

    public String getNodeID() {
        return nodeID;
    }

    public void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }

    /*
    负载分数
     */
    public int getLoadRate()
    {
        return 100;
    }
}
