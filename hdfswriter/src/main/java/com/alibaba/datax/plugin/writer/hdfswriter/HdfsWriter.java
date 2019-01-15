package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.Constant;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class HdfsWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;

        private String defaultFS;
        private String path;
        private String fileType;
        private String fileName;
        private List<Configuration> columns;
        private String writeMode;
        private String fieldDelimiter;
        private String compress;
        private String encoding;
        private HashSet<String> tmpFiles = new HashSet<String>();//临时文件全路径
        private HashSet<String> endFiles = new HashSet<String>();//最终文件全路径

        private HdfsHelper hdfsHelper = null;

        //added by tangye 2017-09-06 for partition writer
        private boolean isPartitionWriter=false;
        private String rootPath;
        private String rootTmpPath;
        private List<Integer> subDirsList=new ArrayList<Integer>();

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();

            //创建textfile存储
            hdfsHelper = new HdfsHelper();

            hdfsHelper.getFileSystem(defaultFS, this.writerSliceConfig);
        }

        private void parsePathForPartitionWriter(JSONObject pathJSON)
        {
            rootPath=pathJSON.getString(Key.ROOT_PATH);
            if(rootPath==null)
            {
                LOG.error("要使用分区写入方式必须配置"+Key.ROOT_PATH+".");
                LOG.info("分区写入方式的path配置参考:\"path\":{\"rootPath\":\"/user/hive/warehouse/writerorc.db/orcfull\",\"colIndexes\":[6,2]},6和2为子分区所取值column的index,是writer里面的column.");
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, "要使用分区写入方式必须配置"+Key.ROOT_PATH+".");
            }

            //this.path=rootPath;

            JSONArray subDirsArr=pathJSON.getJSONArray(Key.SUBDIRS);
            if(subDirsArr==null || subDirsArr.isEmpty())
            {
                LOG.error("要使用分区写入方式必须配置合适的"+Key.SUBDIRS+".");
                LOG.info("分区写入方式的path配置参考:\"path\":{\"rootPath\":\"/user/hive/warehouse/writerorc.db/orcfull\",\"colIndexes\":[6,2]},6和2为子分区所取值column的index.是writer里面的column.");
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, "要使用分区写入方式必须配置合适的"+Key.SUBDIRS+".");
            }

            for(Object obj : subDirsArr)
            {
                subDirsList.add((Integer)obj);
            }

            isPartitionWriter=true;
        }

        private void validateParameter() {
            this.defaultFS = this.writerSliceConfig.getNecessaryValue(Key.DEFAULT_FS, HdfsWriterErrorCode.REQUIRED_VALUE);

            //check hdfs HA conf
            String hdfsHAConf=this.writerSliceConfig.getString(Key.HDFS_HA_CONF);
            if(hdfsHAConf!=null && !hdfsHAConf.trim().equals(""))
            {
                LOG.info(String.format("检验HDFS的HA配置参数: %s", hdfsHAConf));
                try
                {
                    JSONObject hdfsHAConfJson=JSONObject.parseObject(hdfsHAConf);
                }
                catch(Exception e)
                {
                    String message = String.format("请检查参数hdfsHAConf:[%s],需要配置为JSON格式", hdfsHAConf);
                    LOG.error(message);
                    LOG.info("HDFS的HA连接配置参考:{\\\"dfs.nameservices\\\":\\\"nameservice1\\\",\\\"dfs.ha.namenodes.nameservice1\\\":\\\"namenode245,namenode287\\\",\\\"dfs.namenode.rpc-address.nameservice1.namenode245\\\":\\\"******:8020\\\",\\\"dfs.namenode.rpc-address.nameservice1.namenode287\\\":\\\"******:8020\\\",\\\"dfs.client.failover.proxy.provider.nameservice1\\\":\\\"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\\\"}");
                    throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
                }

            }

            //fileType check
            this.fileType = this.writerSliceConfig.getNecessaryValue(Key.FILE_TYPE, HdfsWriterErrorCode.REQUIRED_VALUE);
            if( !fileType.equalsIgnoreCase("ORC") && !fileType.equalsIgnoreCase("TEXT") && !fileType.equalsIgnoreCase("ORC_NEW")){
                String message = "HdfsWriter插件目前只支持ORC和TEXT两种格式的文件,请将filetype选项的值配置为ORC或者TEXT";
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
            }
            //path
            String pathTmp=this.writerSliceConfig.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
            JSONObject pathJSON=null;
            try {
                pathJSON=JSONObject.parseObject(pathTmp);
            }
            catch(Exception e)
            {
                LOG.info("path的值不是JSON格式.");
            }

            if(pathJSON!=null)
            {
                LOG.info("所配置的写入方式为分区写入.");
                parsePathForPartitionWriter(pathJSON);
            }
            else
            {
                LOG.info("所配置的写入方式为普通写入.");
                this.path = this.writerSliceConfig.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
                if(!path.startsWith("/")){
                    String message = String.format("请检查参数path:[%s],需要配置为绝对路径", path);
                    LOG.error(message);
                    throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
                }else if(path.contains("*") || path.contains("?")){
                    String message = String.format("请检查参数path:[%s],不能包含*,?等特殊字符", path);
                    LOG.error(message);
                    throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
                }
            }

            //fileName
            this.fileName = this.writerSliceConfig.getNecessaryValue(Key.FILE_NAME, HdfsWriterErrorCode.REQUIRED_VALUE);
            //columns check
            this.columns = this.writerSliceConfig.getListConfiguration(Key.COLUMN);
            if (null == columns || columns.size() == 0) {
                throw DataXException.asDataXException(HdfsWriterErrorCode.REQUIRED_VALUE, "您需要指定 columns");
            }else{
                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(Key.NAME, HdfsWriterErrorCode.COLUMN_REQUIRED_VALUE);
                    eachColumnConf.getNecessaryValue(Key.TYPE, HdfsWriterErrorCode.COLUMN_REQUIRED_VALUE);
                }
            }
            //writeMode check
            this.writeMode = this.writerSliceConfig.getNecessaryValue(Key.WRITE_MODE, HdfsWriterErrorCode.REQUIRED_VALUE);
            writeMode = writeMode.toLowerCase().trim();
            Set<String> supportedWriteModes = Sets.newHashSet("append", "nonconflict");
            if (!supportedWriteModes.contains(writeMode)) {
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("仅支持append, nonConflict两种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                writeMode));
            }
            this.writerSliceConfig.set(Key.WRITE_MODE, writeMode);
            //fieldDelimiter check
            this.fieldDelimiter = this.writerSliceConfig.getString(Key.FIELD_DELIMITER,null);
            if(null == fieldDelimiter){
                throw DataXException.asDataXException(HdfsWriterErrorCode.REQUIRED_VALUE,
                        String.format("您提供配置文件有误，[%s]是必填参数.", Key.FIELD_DELIMITER));
            }else if(1 != fieldDelimiter.length()){
                // warn: if have, length must be one
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", fieldDelimiter));
            }
            //compress check
            this.compress  = this.writerSliceConfig.getString(Key.COMPRESS,null);
            if(fileType.equalsIgnoreCase("TEXT")){
                Set<String> textSupportedCompress = Sets.newHashSet("GZIP", "BZIP2");
                //用户可能配置的是compress:"",空字符串,需要将compress设置为null
                if(StringUtils.isBlank(compress) ){
                    this.writerSliceConfig.set(Key.COMPRESS, null);
                }else {
                    compress = compress.toUpperCase().trim();
                    if(!textSupportedCompress.contains(compress) ){
                        throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                                String.format("目前TEXT FILE仅支持GZIP、BZIP2 两种压缩, 不支持您配置的 compress 模式 : [%s]",
                                        compress));
                    }
                }
            }else if(fileType.equalsIgnoreCase("ORC") || fileType.equalsIgnoreCase("ORC_NEW")){
                Set<String> orcSupportedCompress = Sets.newHashSet("NONE", "ZLIB", "LZO", "SNAPPY");
                if(null == compress){
                    this.writerSliceConfig.set(Key.COMPRESS, "NONE");
                }else {
                    compress = compress.toUpperCase().trim();
                    if(!orcSupportedCompress.contains(compress)){
                        throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                                String.format("目前ORC FILE仅支持ZLIB,LZO,SNAPPY压缩, 不支持您配置的 compress 模式 : [%s]",
                                        compress));
                    }
                }

            }
            //Kerberos check
            Boolean haveKerberos = this.writerSliceConfig.getBool(Key.HAVE_KERBEROS, false);
            if(haveKerberos) {
                this.writerSliceConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
                this.writerSliceConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HdfsWriterErrorCode.REQUIRED_VALUE);
            }
            // encoding check
            this.encoding = this.writerSliceConfig.getString(Key.ENCODING,Constant.DEFAULT_ENCODING);
            try {
                encoding = encoding.trim();
                this.writerSliceConfig.set(Key.ENCODING, encoding);
                Charsets.toCharset(encoding);
            } catch (Exception e) {
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("不支持您配置的编码格式:[%s]", encoding), e);
            }
        }

        @Override
        public void prepare() {
            if(!isPartitionWriter)
            {
                //若路径已经存在，检查path是否是目录
                if(hdfsHelper.isPathexists(path)){
                    if(!hdfsHelper.isPathDir(path)){
                        throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                                String.format("您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                        path));
                    }
                    //根据writeMode对目录下文件进行处理
                    Path[] existFilePaths = hdfsHelper.hdfsDirList(path,fileName);
                    boolean isExistFile = false;
                    if(existFilePaths.length > 0){
                        isExistFile = true;
                    }
                    /**
                     if ("truncate".equals(writeMode) && isExistFile ) {
                     LOG.info(String.format("由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容",
                     path, fileName));
                     hdfsHelper.deleteFiles(existFilePaths);
                     } else
                     */
                    if ("append".equalsIgnoreCase(writeMode)) {
                        LOG.info(String.format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
                                path, fileName));
                    } else if ("nonconflict".equalsIgnoreCase(writeMode) && isExistFile) {
                        LOG.info(String.format("由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));
                        List<String> allFiles = new ArrayList<String>();
                        for (Path eachFile : existFilePaths) {
                            allFiles.add(eachFile.toString());
                        }
                        LOG.error(String.format("冲突文件列表为: [%s]", StringUtils.join(allFiles, ",")));
                        throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                                String.format("由于您配置了writeMode nonConflict,但您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.", path));
                    }
                }else{
                    throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("您配置的path: [%s] 不存在, 请先在hive端创建对应的数据库和表.", path));
                }
            }
        }

        @Override
        public void post() {
            //删除临时目录
            if(this.isPartitionWriter)
            {
                LOG.info("开始把临时目录 "+this.rootTmpPath+" 改名为正式目录 "+this.rootPath+" .");
                hdfsHelper.moveTo(this.rootTmpPath,this.rootPath);
            }
            else
            {
                if("nonconflict".equalsIgnoreCase(writeMode))
                {
                    LOG.info(String.format("由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容,将先清空目录,以避免还有别的进程也在写入数据而引起数据重复.", path));
                    Path[] existFilePaths = hdfsHelper.hdfsDirList(path,fileName);
                    boolean isExistFile = false;
                    if(existFilePaths.length > 0){
                        isExistFile = true;
                    }

                    if(isExistFile)
                    {
                        List<String> allFiles = new ArrayList<String>();
                        for (Path eachFile : existFilePaths) {
                            allFiles.add(eachFile.toString());
                        }
                        LOG.info(String.format("将要被清空的文件列表为: [%s]", StringUtils.join(allFiles, ",")));
                        hdfsHelper.deleteFiles(existFilePaths);
                    }
                }

                hdfsHelper.renameFile(tmpFiles, endFiles);
            }
        }

        @Override
        public void destroy() {
            hdfsHelper.closeFileSystem();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();

            if(!isPartitionWriter)
            {
                String filePrefix = fileName;

                Set<String> allFiles = new HashSet<String>();

                //获取该路径下的所有已有文件列表
                if(hdfsHelper.isPathexists(path)){
                    allFiles.addAll(Arrays.asList(hdfsHelper.hdfsDirList(path)));
                }

                String fileSuffix;
                //临时存放路径
                String storePath =  buildTmpFilePath(this.path);
                //最终存放路径
                String endStorePath = buildFilePath(this.path);
                this.path = endStorePath;
                for (int i = 0; i < mandatoryNumber; i++) {
                    // handle same file name

                    Configuration splitedTaskConfig = this.writerSliceConfig.clone();
                    String fullFileName = null;
                    String endFullFileName = null;

                    fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                    fullFileName = String.format("%s%s%s__%s", defaultFS, storePath, filePrefix, fileSuffix);
                    endFullFileName = String.format("%s%s%s__%s", defaultFS, endStorePath, filePrefix, fileSuffix);

                    //确保文件不重复
                    while (allFiles.contains(endFullFileName)) {
                        fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                        fullFileName = String.format("%s%s%s__%s", defaultFS, storePath, filePrefix, fileSuffix);
                        endFullFileName = String.format("%s%s%s__%s", defaultFS, endStorePath, filePrefix, fileSuffix);
                    }
                    allFiles.add(endFullFileName);

                    //设置临时文件全路径和最终文件全路径
                    if("GZIP".equalsIgnoreCase(this.compress)){
                        this.tmpFiles.add(fullFileName + ".gz");
                        this.endFiles.add(endFullFileName + ".gz");
                    }else if("BZIP2".equalsIgnoreCase(compress)){
                        this.tmpFiles.add(fullFileName + ".bz2");
                        this.endFiles.add(endFullFileName + ".bz2");
                    }else{
                        this.tmpFiles.add(fullFileName);
                        this.endFiles.add(endFullFileName);
                    }

                    splitedTaskConfig
                            .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                                    fullFileName);

                    LOG.info(String.format("splited write file name:[%s]",
                            fullFileName));

                    writerSplitConfigs.add(splitedTaskConfig);
                }
            }
            else
            {
                this.rootTmpPath=buildTmpFilePath(this.rootPath);

                for (int i = 0; i < mandatoryNumber; i++) {
                    Configuration splitedTaskConfig = this.writerSliceConfig.clone();
                    splitedTaskConfig.set(Key.IS_PARTITION_WRITER,true);
                    splitedTaskConfig.set(Key.ROOT_PATH,this.rootPath);
                    splitedTaskConfig.set(Key.ROOT_TMP_PATH,this.rootTmpPath);
                    splitedTaskConfig.set(Key.SUBDIRS,this.subDirsList);
                    writerSplitConfigs.add(splitedTaskConfig);
                }
            }

            LOG.info("end do split.");
            return writerSplitConfigs;
        }

        public static String buildFilePath(String path) {
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
                case IOUtils.DIR_SEPARATOR_UNIX:
                    isEndWithSeparator = path.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR));
                    break;
                case IOUtils.DIR_SEPARATOR_WINDOWS:
                    isEndWithSeparator = path.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                    break;
                default:
                    break;
            }
            if (!isEndWithSeparator) {
                path = path + IOUtils.DIR_SEPARATOR;
            }
            return path;
        }

        /**
         * 创建临时目录
         * @param userPath
         * @return
         */
        private String buildTmpFilePath(String userPath) {
            String tmpFilePath;
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
                case IOUtils.DIR_SEPARATOR_UNIX:
                    isEndWithSeparator = userPath.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR));
                    break;
                case IOUtils.DIR_SEPARATOR_WINDOWS:
                    isEndWithSeparator = userPath.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                    break;
                default:
                    break;
            }
            String tmpSuffix;
            tmpSuffix = UUID.randomUUID().toString().replace('-', '_');
            if (!isEndWithSeparator) {
                tmpFilePath = String.format("%s__%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
            }else if("/".equals(userPath)){
                tmpFilePath = String.format("%s__%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
            }else{
                tmpFilePath = String.format("%s__%s%s", userPath.substring(0,userPath.length()-1), tmpSuffix, IOUtils.DIR_SEPARATOR);
            }
            while(hdfsHelper.isPathexists(tmpFilePath)){
                tmpSuffix = UUID.randomUUID().toString().replace('-', '_');
                if (!isEndWithSeparator) {
                    tmpFilePath = String.format("%s__%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
                }else if("/".equals(userPath)){
                    tmpFilePath = String.format("%s__%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
                }else{
                    tmpFilePath = String.format("%s__%s%s", userPath.substring(0,userPath.length()-1), tmpSuffix, IOUtils.DIR_SEPARATOR);
                }
            }
            return tmpFilePath;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String defaultFS;
        private String fileType;
        private String fileName;

        private HdfsHelper hdfsHelper = null;

        //added by tangye 2017-09-06 for partition writer
        private boolean isPartitionWriter=false;
        private String rootPath;
        private String rootTmpPath;
        private List<Integer> subDirsList;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();

            this.defaultFS = this.writerSliceConfig.getString(Key.DEFAULT_FS);
            this.fileType = this.writerSliceConfig.getString(Key.FILE_TYPE);
            //得当的已经是绝对路径，eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile
            this.fileName = this.writerSliceConfig.getString(Key.FILE_NAME);

            hdfsHelper = new HdfsHelper();
            hdfsHelper.getFileSystem(defaultFS, writerSliceConfig);

            this.isPartitionWriter=this.writerSliceConfig.getBool(Key.IS_PARTITION_WRITER,false);
            this.rootPath=this.writerSliceConfig.getString(Key.ROOT_PATH);
            this.rootTmpPath=this.writerSliceConfig.getString(Key.ROOT_TMP_PATH);
            this.subDirsList=this.writerSliceConfig.getList(Key.SUBDIRS,Integer.class);
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            LOG.info(String.format("write to file : [%s]", this.fileName));
            if(fileType.equalsIgnoreCase("TEXT")){
                //写TEXT FILE
                hdfsHelper.textFileStartWrite(lineReceiver,this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            }else if(fileType.equalsIgnoreCase("ORC")){
                //写ORC FILE
                hdfsHelper.orcFileStartWrite(lineReceiver,this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            }else if(fileType.equalsIgnoreCase("ORC_NEW")){
                LOG.info("启用了新版ORC文件写入功能,将获得更高的性能.");
                if(this.isPartitionWriter)
                {
                    LOG.info("使用分区写入的方式.");
                    hdfsHelper.orcFileStartWriteByPartition(lineReceiver,this.writerSliceConfig, this.rootTmpPath,subDirsList,
                            this.getTaskPluginCollector());
                }
                else
                {
                    hdfsHelper.orcFileStartWriteNew(lineReceiver,this.writerSliceConfig, this.fileName,
                            this.getTaskPluginCollector());
                }
            }

            LOG.info("end do write");
        }

        @Override
        public void post() {
            //如果是分区方式写入,则rename临时文件到目标文件,但是不能删除临时目录,因为别的split还可能在用,让job来删除临时目录
            if(this.isPartitionWriter)
            {

            }
        }

        @Override
        public void destroy() {

        }
    }
}
