package com.alibaba.datax.server.work;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.datax.server.communicator.communication.Communication;
import com.alibaba.datax.server.plugin.AbstractTaskPluginCollector;
import com.alibaba.datax.server.runner.AbstractRunner;
import com.alibaba.datax.server.runner.ReaderRunner;
import com.alibaba.datax.server.runner.WriterRunner;
import com.alibaba.datax.server.transport.channel.Channel;
import com.alibaba.datax.server.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.datax.server.transport.exchanger.BufferedRecordTransformerExchanger;
import com.alibaba.datax.server.transport.transformer.TransformerExecution;
import com.alibaba.datax.server.util.*;
import org.apache.commons.lang3.Validate;

import java.util.List;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public class Task extends AbstractWork {

    private int attemptCount;

    private Channel channel;

    private Thread readerThread;

    private Thread writerThread;

    private ReaderRunner readerRunner;

    private WriterRunner writerRunner;

    private String channelClazz;
    private String taskCollectorClass;

    /**
     * 该处的taskCommunication在多处用到：
     * 1. channel
     * 2. readerRunner和writerRunner
     * 3. reader和writer的taskPluginCollector
     */
    //private Communication taskCommunication;

    @Override
    public void addSchedule() {
        this.writerThread.start();

        // reader没有起来，writer不可能结束
        if (!this.writerThread.isAlive() || this.getCommunication().getState() == State.FAILED) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    this.getCommunication().getThrowable());
        }

        this.readerThread.start();

        // 这里reader可能很快结束
        if (!this.readerThread.isAlive() && this.getCommunication().getState() == State.FAILED) {
            // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    this.getCommunication().getThrowable());
        }

    }


    private AbstractRunner generateRunner(PluginType pluginType) {
        return generateRunner(pluginType, null);
    }

    private AbstractRunner generateRunner(PluginType pluginType, List<TransformerExecution> transformerInfoExecs) {
        AbstractRunner newRunner = null;
        TaskPluginCollector pluginCollector;

        switch (pluginType) {
            case READER:
                newRunner = LoadUtil.loadPluginRunner(pluginType,
                        this.getConfiguration().getString(CoreConstant.JOB_READER_NAME));
                newRunner.setJobConf(this.getConfiguration().getConfiguration(
                        CoreConstant.JOB_READER_PARAMETER));

                //Communication用于统计Collector中的脏数据
                pluginCollector = ClassUtil.instantiate(
                        taskCollectorClass, AbstractTaskPluginCollector.class,
                        this.getConfiguration(), this.getCommunication(),
                        PluginType.READER);

                RecordSender recordSender;
                if (transformerInfoExecs != null && transformerInfoExecs.size() > 0) {
                    //Communication用于统计Transformer处理数据的信息
                    recordSender = new BufferedRecordTransformerExchanger(this.getContainer().getSourceWork().getWorkID(), this.getWorkID(), this.channel,this.getCommunication() ,pluginCollector, transformerInfoExecs);
                } else {
                    recordSender = new BufferedRecordExchanger(this.channel, pluginCollector);
                }

                ((ReaderRunner) newRunner).setRecordSender(recordSender);

                /**
                 * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                 */
                newRunner.setTaskPluginCollector(pluginCollector);
                break;
            case WRITER:
                newRunner = LoadUtil.loadPluginRunner(pluginType,
                        this.getConfiguration().getString(CoreConstant.JOB_WRITER_NAME));
                newRunner.setJobConf(this.getConfiguration()
                        .getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));

                pluginCollector = ClassUtil.instantiate(
                        taskCollectorClass, AbstractTaskPluginCollector.class,
                        this.getConfiguration(), this.getCommunication(),
                        PluginType.WRITER);
                ((WriterRunner) newRunner).setRecordReceiver(new BufferedRecordExchanger(
                        this.channel, pluginCollector));
                /**
                 * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                 */
                newRunner.setTaskPluginCollector(pluginCollector);
                break;
            default:
                throw DataXException.asDataXException(FrameworkErrorCode.ARGUMENT_ERROR, "Cant generateRunner for:" + pluginType);
        }

        newRunner.setTaskGroupId(this.getContainer().getSourceWork().getWorkID());
        newRunner.setTaskId(this.getWorkID());
        newRunner.setRunnerCommunication(this.getCommunication());

        return newRunner;
    }

    // 检查任务是否结束
    @Override
    public boolean isFinished() {
        // 如果reader 或 writer没有完成工作，那么直接返回工作没有完成
        if (readerThread.isAlive() || writerThread.isAlive()) {
            return false;
        }

        if(getCommunication()==null || !getCommunication().isFinished()){
            return false;
        }

        return true;
    }

    //public long getTimeStamp(){
        //return getCommunication().getTimestamp();
    //}

    //public int getAttemptCount(){
        //return attemptCount;
    //}

    @Override
    public boolean supportFailOver(){
        return writerRunner.supportFailOver();
    }

    /*@Override
    public boolean isZKAssign() {
        return false;
    }*/

    @Override
    public void shutdown(){
        writerRunner.shutdown();
        readerRunner.shutdown();
        if(writerThread.isAlive()){
            writerThread.interrupt();
        }
        if(readerThread.isAlive()){
            readerThread.interrupt();
        }
    }

    @Override
    public boolean isShutdown(){
        return !readerThread.isAlive() && !writerThread.isAlive();
    }

    public Task(Configuration configuration) {
        super(configuration);

        this.channelClazz = configuration.getString(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);
        this.taskCollectorClass = configuration.getString(
                CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
    }

    @Override
    public Communication initCommunication() {
        return new Communication();
    }

    @Override
    public void addInit() throws Exception {
        Validate.isTrue(null != this.getConfiguration().getConfiguration(CoreConstant.JOB_READER)
                        && null != this.getConfiguration().getConfiguration(CoreConstant.JOB_WRITER),
                "[reader|writer]的插件参数不能为空!");

        // 得到taskId
        //this.setWorkID(this.getConfiguration().getInt(CoreConstant.TASK_ID));

        /**
         * 由taskId得到该taskExecutor的Communication
         * 要传给readerRunner和writerRunner，同时要传给channel作统计用
         */
        //this.taskCommunication = containerCommunicator
        //.getCommunication(taskId);
        Validate.notNull(this.getCommunication(),
                String.format("taskId[%d]的Communication没有注册过", this.getWorkID()));
        this.channel = ClassUtil.instantiate(channelClazz,
                Channel.class, this.getConfiguration());
        this.channel.setCommunication(this.getCommunication());

        /**
         * 获取transformer的参数
         */

        List<TransformerExecution> transformerInfoExecs = TransformerUtil.buildTransformerInfo(getConfiguration());

        /**
         * 生成writerThread
         */
        writerRunner = (WriterRunner) generateRunner(PluginType.WRITER);
        this.writerThread = new Thread(writerRunner,
                String.format("%d-%d-writer",this.getContainer().getSourceWork().getWorkID(), this.getWorkID()));
        //通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
        this.writerThread.setContextClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.getConfiguration().getString(
                        CoreConstant.JOB_WRITER_NAME)));

        /**
         * 生成readerThread
         */
        readerRunner = (ReaderRunner) generateRunner(PluginType.READER,transformerInfoExecs);
        this.readerThread = new Thread(readerRunner,
                String.format("%d-%d-reader",this.getContainer().getSourceWork().getWorkID(), this.getWorkID()));
        /**
         * 通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
         */
        this.readerThread.setContextClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.getConfiguration().getString(
                        CoreConstant.JOB_READER_NAME)));
    }

    @Override
    public void addCheck() throws Exception {

    }

    @Override
    public void addPrepare() throws Exception {

    }

    @Override
    public void addPost() throws Exception {

    }

    @Override
    public void addDestroy() throws Exception {

    }

    @Override
    public List<AbstractWork> split() {
        return null;
    }
}
