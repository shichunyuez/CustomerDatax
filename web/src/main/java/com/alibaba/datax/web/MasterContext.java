package com.alibaba.datax.web;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class MasterContext {

	private static final Logger LOG = LoggerFactory.getLogger(MasterContext.class);

	private Map<Channel, WorkerHolder> workers=new ConcurrentHashMap<Channel, WorkerHolder>();
	private ApplicationContext applicationContext;

	private MasterHandler networkHandler;
	private MasterServer networkServer;
	private ExecutorService threadPool=Executors.newCachedThreadPool();
	private ScheduledExecutorService schedulePool=Executors.newScheduledThreadPool(5);
	
	public MasterContext(ApplicationContext applicationContext){
		this.applicationContext=applicationContext;
	}
	public void init(int port){
		networkHandler=new MasterHandler(this);
		networkServer=new MasterServer(networkHandler);
		networkServer.start(port);
	}
	public void destory(){
		threadPool.shutdown();
		schedulePool.shutdown();
		if(networkServer!=null){
			networkServer.shutdown();
		}
	}
	
	public Map<Channel, WorkerHolder> getWorkers() {
		return workers;
	}

}
