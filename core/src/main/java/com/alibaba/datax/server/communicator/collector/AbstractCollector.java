package com.alibaba.datax.server.communicator.collector;

import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.datax.server.communicator.communication.Communication;
import com.alibaba.datax.server.work.AbstractWork;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public abstract class AbstractCollector {

    protected Map<Integer, Communication> communicationMap = new ConcurrentHashMap<Integer, Communication>();

    public AbstractCollector(List<AbstractWork> works)
    {
        for(AbstractWork work : works)
        {
            this.communicationMap.put(work.getWorkID(),work.getCommunication());
        }
    }

    public abstract Communication collect(List<AbstractWork> works);
}
