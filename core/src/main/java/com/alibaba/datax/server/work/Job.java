package com.alibaba.datax.server.work;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.JobPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.server.DefaultJobPluginCollector;
import com.alibaba.datax.server.communicator.communication.Communication;
import com.alibaba.datax.server.communicator.communication.DistributeCommunication;
import com.alibaba.datax.server.container.JobContainer;
import com.alibaba.datax.server.resource.NodeManager;
import com.alibaba.datax.server.util.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/7/26 0026.
 */
public class Job extends AbstractWork {

    private static final Logger LOG = LoggerFactory
            .getLogger(Job.class);

    private Reader.Job jobReader;
    private Writer.Job jobWriter;

    private String readerPluginName;

    private String writerPluginName;

    private ErrorRecordChecker errorLimit;

    private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper
            .newCurrentThreadClassLoaderSwapper();

    public Job(Configuration configuration) {
        super(configuration);
        errorLimit = new ErrorRecordChecker(configuration);
    }

    @Override
    public Communication initCommunication() {
        return new DistributeCommunication("");
    }

    @Override
    public boolean supportFailOver() {
        return false;
    }

    /*@Override
    public boolean isZKAssign() {
        return false;
    }*/

    @Override
    public void addSchedule() {
        NodeManager.getLocalNodeManager().assign(this);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isFinished() {
        return false;
    }

    public JobContainer getJobContainer() {
        return (JobContainer)getContainer();
    }

    private void preCheckReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do preCheck work .",
                this.readerPluginName));
        this.jobReader.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void preCheckWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do preCheck work .",
                this.writerPluginName));
        this.jobWriter.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void preCheck()
    {
        this.preCheckReader();
        this.preCheckWriter();
        LOG.info("PreCheck通过");
    }

    private void preHandle() {
        String handlerPluginTypeStr = this.getConfiguration().getString(
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINTYPE);
        if(!StringUtils.isNotEmpty(handlerPluginTypeStr)){
            return;
        }
        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job preHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = this.getConfiguration().getString(
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                handlerPluginType, handlerPluginName));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(
                handlerPluginType, handlerPluginName);

        //JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                //this.getJobContainer().getCommunicator());
        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(null);
        handler.setJobPluginCollector(jobPluginCollector);

        //todo configuration的安全性，将来必须保证
        handler.preHandler(this.getConfiguration());
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        LOG.info("After PreHandler: \n" + ConfigurationUtil.filterJobConfiguration(this.getConfiguration()) + "\n");
    }

    private Reader.Job initJobReader(
            JobPluginCollector jobPluginCollector) {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(
                PluginType.READER, this.readerPluginName);

        // 设置reader的jobConfig
        jobReader.setPluginJobConf(this.getConfiguration().getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        // 设置reader的writerConfig
        jobReader.setPeerPluginJobConf(this.getConfiguration().getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));

        jobReader.setJobPluginCollector(jobPluginCollector);
        jobReader.init();

        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }

    /**
     * writer job的初始化，返回Writer.Job
     *
     * @return
     */
    private Writer.Job initJobWriter(
            JobPluginCollector jobPluginCollector) {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(
                PluginType.WRITER, this.writerPluginName);

        // 设置writer的jobConfig
        jobWriter.setPluginJobConf(this.getConfiguration().getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));

        // 设置writer的readerConfig
        jobWriter.setPeerPluginJobConf(this.getConfiguration().getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);
        jobWriter.init();
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    @Override
    public void addInit() {
        this.readerPluginName = this.getConfiguration().getString(CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        this.writerPluginName = this.getConfiguration().getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);

        Thread.currentThread().setName("job-" + this.getWorkID());

        //JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getJobContainer().getCommunicator());
        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(null);
        //必须先Reader ，后Writer
        this.jobReader = this.initJobReader(jobPluginCollector);
        this.jobWriter = this.initJobWriter(jobPluginCollector);

        this.preHandle();
    }

    @Override
    public void addCheck() {
        this.preCheck();
    }

    private void prepareJobReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do prepare work .",
                this.readerPluginName));
        this.jobReader.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void prepareJobWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do prepare work .",
                this.writerPluginName));
        this.jobWriter.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    @Override
    public void addPrepare() {
        this.prepareJobReader();
        this.prepareJobWriter();
    }

    private int adjustChannelNumber() {
        int needChannelNumberByByte = Integer.MAX_VALUE;
        int needChannelNumberByRecord = Integer.MAX_VALUE;

        boolean isByteLimit = (this.getConfiguration().getInt(
                CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 0) > 0);
        if (isByteLimit) {
            long globalLimitedByteSpeed = this.getConfiguration().getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 10 * 1024 * 1024);

            // 在byte流控情况下，单个Channel流量最大值必须设置，否则报错！
            Long channelLimitedByteSpeed = this.getConfiguration()
                    .getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE);
            if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0) {
                DataXException.asDataXException(
                        FrameworkErrorCode.CONFIG_ERROR,
                        "在有总bps限速条件下，单个channel的bps值不能为空，也不能为非正数");
            }

            needChannelNumberByByte =
                    (int) (globalLimitedByteSpeed / channelLimitedByteSpeed);
            needChannelNumberByByte =
                    needChannelNumberByByte > 0 ? needChannelNumberByByte : 1;
            LOG.info("Job set Max-Byte-Speed to " + globalLimitedByteSpeed + " bytes.");
        }

        boolean isRecordLimit = (this.getConfiguration().getInt(
                CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 0)) > 0;
        if (isRecordLimit) {
            long globalLimitedRecordSpeed = this.getConfiguration().getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 100000);

            Long channelLimitedRecordSpeed = this.getConfiguration().getLong(
                    CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD);
            if (channelLimitedRecordSpeed == null || channelLimitedRecordSpeed <= 0) {
                DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                        "在有总tps限速条件下，单个channel的tps值不能为空，也不能为非正数");
            }

            needChannelNumberByRecord =
                    (int) (globalLimitedRecordSpeed / channelLimitedRecordSpeed);
            needChannelNumberByRecord =
                    needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;
            LOG.info("Job set Max-Record-Speed to " + globalLimitedRecordSpeed + " records.");
        }

        // 取较小值
        int needChannelNumber = needChannelNumberByByte < needChannelNumberByRecord ?
                needChannelNumberByByte : needChannelNumberByRecord;

        // 如果从byte或record上设置了needChannelNumber则退出
        if (needChannelNumber < Integer.MAX_VALUE) {
            return needChannelNumber;
        }

        boolean isChannelLimit = (this.getConfiguration().getInt(
                CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL, 0) > 0);
        if (isChannelLimit) {
            needChannelNumber = this.getConfiguration().getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL);

            LOG.info("Job set Channel-Number to " + needChannelNumber
                    + " channels.");

            return needChannelNumber;
        }

        throw DataXException.asDataXException(
                FrameworkErrorCode.CONFIG_ERROR,
                "Job运行速度必须设置");
    }

    private List<Configuration> doReaderSplit(int adviceNumber) {
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info("Begin to do split in Reader.");
        List<Configuration> readerSlicesConfigs =
                this.jobReader.split(adviceNumber);
        if (readerSlicesConfigs == null || readerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "reader切分的task数目不能小于等于0");
        }
        LOG.info("DataX Reader.Job [{}] splits to [{}] tasks.",
                this.readerPluginName, readerSlicesConfigs.size());
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
        return readerSlicesConfigs;
    }

    private List<Configuration> doWriterSplit(int readerTaskNumber) {
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));

        List<Configuration> writerSlicesConfigs = this.jobWriter
                .split(readerTaskNumber);
        if (writerSlicesConfigs == null || writerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "writer切分的task不能小于等于0");
        }
        LOG.info("DataX Writer.Job [{}] splits to [{}] tasks.",
                this.writerPluginName, writerSlicesConfigs.size());
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();

        return writerSlicesConfigs;
    }

    private List<Configuration> mergeReaderAndWriterTaskConfigs(
            List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs,
            List<Configuration> transformerConfigs) {
        if (readerTasksConfigs.size() != writerTasksConfigs.size()) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    String.format("reader切分的task数目[%d]不等于writer切分的task数目[%d].",
                            readerTasksConfigs.size(), writerTasksConfigs.size())
            );
        }

        List<Configuration> contentConfigs = new ArrayList<Configuration>();
        for (int i = 0; i < readerTasksConfigs.size(); i++) {
            Configuration taskConfig = Configuration.newDefault();
            taskConfig.set(CoreConstant.JOB_READER_NAME,
                    this.readerPluginName);
            taskConfig.set(CoreConstant.JOB_READER_PARAMETER,
                    readerTasksConfigs.get(i));
            taskConfig.set(CoreConstant.JOB_WRITER_NAME,
                    this.writerPluginName);
            taskConfig.set(CoreConstant.JOB_WRITER_PARAMETER,
                    writerTasksConfigs.get(i));

            if(transformerConfigs!=null && transformerConfigs.size()>0){
                taskConfig.set(CoreConstant.JOB_TRANSFORMER, transformerConfigs);
            }

            taskConfig.set(CoreConstant.TASK_ID, i);
            contentConfigs.add(taskConfig);
        }

        return contentConfigs;
    }

    @Override
    public List<AbstractWork> split() {
        int needChannelNumber=this.adjustChannelNumber();

        if (needChannelNumber <= 0) {
            needChannelNumber = 1;
        }

        List<Configuration> readerTaskConfigs = this
                .doReaderSplit(needChannelNumber);
        //LOG.info("Reader Task配置如下: "+ JSON.toJSONString(readerTaskConfigs));
        int readerTaskNumber = readerTaskConfigs.size();
        List<Configuration> writerTaskConfigs = this
                .doWriterSplit(readerTaskNumber);

        //LOG.info("Writer Task配置如下: "+ JSON.toJSONString(writerTaskConfigs));

        List<Configuration> transformerList = this.getConfiguration().getListConfiguration(CoreConstant.DATAX_JOB_CONTENT_TRANSFORMER);

        //LOG.info("Transformer配置如下: "+ JSON.toJSONString(transformerList));
        /**
         * 输入是reader和writer的parameter list，输出是content下面元素的list
         */
        List<Configuration> contentConfig = mergeReaderAndWriterTaskConfigs(
                readerTaskConfigs, writerTaskConfigs, transformerList);

        this.getConfiguration().set(CoreConstant.DATAX_JOB_CONTENT, contentConfig);

        /**
         * 这里的全局speed和每个channel的速度设置为B/s
         */
        int channelsPerTaskGroup = this.getConfiguration().getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, 5);
        int taskNumber = contentConfig.size();

        needChannelNumber = Math.min(needChannelNumber, taskNumber);
        PerfTrace.getInstance().setChannelNumber(needChannelNumber);

        /**
         * 通过获取配置信息得到每个taskGroup需要运行哪些tasks任务
         */

        List<Configuration> taskGroupConfigs = JobAssignUtil.assignFairly(this.getConfiguration(),
                needChannelNumber, channelsPerTaskGroup);

        this.getSubWorks().clear();
        for(Configuration taskGroupConf : taskGroupConfigs)
        {
            this.getSubWorks().add(new TaskGroup(taskGroupConf));
        }

        return this.getSubWorks();
    }

    private void checkLimit() {
        //Communication communication = this.getJobContainer().getCommunicator().collect();
        //errorLimit.checkRecordLimit(communication);
        //errorLimit.checkPercentageLimit(communication);
    }

    private void postJobReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info("DataX Reader.Job [{}] do post work.",
                this.readerPluginName);
        this.jobReader.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void postJobWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info("DataX Writer.Job [{}] do post work.",
                this.writerPluginName);
        this.jobWriter.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    @Override
    public void addPost() {
        this.postJobWriter();
        this.postJobReader();
    }

    @Override
    public void addDestroy() {
        if (this.jobWriter != null) {
            this.jobWriter.destroy();
            this.jobWriter = null;
        }
        if (this.jobReader != null) {
            this.jobReader.destroy();
            this.jobReader = null;
        }
    }
}