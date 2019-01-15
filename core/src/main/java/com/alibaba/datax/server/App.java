package com.alibaba.datax.server;

/**
 * Created by Administrator on 2017/7/27 0027.
 */
public interface App {

    void addInit() throws Exception;

    void addCheck() throws Exception;

    void addPrepare() throws Exception;

    void addSchedule() throws Exception;

    void addPost() throws Exception;

    void addDestroy() throws Exception;

}
