package com.alibaba.datax.server.communicator.reporter;

import com.alibaba.datax.server.communicator.communication.Communication;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public abstract class AbstractReporter {

    public abstract Communication report(Communication communication);
}
