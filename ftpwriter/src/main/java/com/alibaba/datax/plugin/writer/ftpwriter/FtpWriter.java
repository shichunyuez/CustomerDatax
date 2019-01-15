package com.alibaba.datax.plugin.writer.ftpwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.TextCsvWriterManager;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.alibaba.datax.plugin.writer.ftpwriter.util.Constant;
import com.alibaba.datax.plugin.writer.ftpwriter.util.IFtpHelper;
import com.alibaba.datax.plugin.writer.ftpwriter.util.SftpHelperImpl;
import com.alibaba.datax.plugin.writer.ftpwriter.util.StandardFtpHelperImpl;

import com.jcraft.jsch.SftpException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FtpWriter extends Writer {
    private static Boolean assChar;
    private static String prefuName="";
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private Set<String> allFileExists = null;

        private String protocol;
        private String host;
        private int port;
        private String username;
        private String password;
        private int timeout;
        private IFtpHelper ftpHelper = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            UnstructuredStorageWriterUtil
                    .validateParameter(this.writerSliceConfig);
            try {
                RetryUtil.executeWithRetry(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        ftpHelper.loginFtpServer(host, username, password,
                                port, timeout);
                        return null;
                    }
                }, 3, 4000, true);
            } catch (Exception e) {
                String message = String
                        .format("与ftp服务器建立连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                                host, username, port, e.getMessage());
                LOG.error(message);
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.FAIL_LOGIN, message, e);
            }
        }

        private void validateParameter() {
            this.writerSliceConfig
                    .getNecessaryValue(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                            FtpWriterErrorCode.REQUIRED_VALUE);
            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH,
                    FtpWriterErrorCode.REQUIRED_VALUE);
            if (!path.startsWith("/")) {
                String message = String.format("请检查参数path:%s,需要配置为绝对路径", path);
                LOG.error(message);
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.ILLEGAL_VALUE, message);
            }

            this.host = this.writerSliceConfig.getNecessaryValue(Key.HOST,
                    FtpWriterErrorCode.REQUIRED_VALUE);
            this.username = this.writerSliceConfig.getNecessaryValue(
                    Key.USERNAME, FtpWriterErrorCode.REQUIRED_VALUE);
            this.password = this.writerSliceConfig.getNecessaryValue(
                    Key.PASSWORD, FtpWriterErrorCode.REQUIRED_VALUE);
            this.timeout = this.writerSliceConfig.getInt(Key.TIMEOUT,
                    Constant.DEFAULT_TIMEOUT);

            this.protocol = this.writerSliceConfig.getNecessaryValue(
                    Key.PROTOCOL, FtpWriterErrorCode.REQUIRED_VALUE);
            if ("sftp".equalsIgnoreCase(this.protocol)) {
                this.port = this.writerSliceConfig.getInt(Key.PORT,
                        Constant.DEFAULT_SFTP_PORT);
                this.ftpHelper = new SftpHelperImpl();
            } else if ("ftp".equalsIgnoreCase(this.protocol)) {
                this.port = this.writerSliceConfig.getInt(Key.PORT,
                        Constant.DEFAULT_FTP_PORT);
                this.ftpHelper = new StandardFtpHelperImpl();
            } else {
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.ILLEGAL_VALUE, String.format(
                                "仅支持 ftp和sftp 传输协议 , 不支持您配置的传输协议: [%s]",
                                protocol));
            }
            this.writerSliceConfig.set(Key.PORT, this.port);
        }

        @Override
        public void prepare() {
            String path = this.writerSliceConfig.getString(Key.PATH);
            // warn: 这里用户需要配一个目录
            this.ftpHelper.mkDirRecursive(path);

            String fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);

            Set<String> allFileExists = this.ftpHelper.getAllFilesInDir(path,
                    fileName);
            this.allFileExists = allFileExists;

            LOG.info("设置ASSCHAR默认为false");
            assChar=false;

            LOG.info(String.format(
                    "匹配[%s] 中是否带有分配字符 ",
                    fileName));
            //匹配fileName中是否带有分配字符
            Pattern pattern = Pattern.compile(":\\d+:");
            Matcher matcher=pattern.matcher(fileName);
            prefuName=fileName;
            while(matcher.find()){
                LOG.info(String.format( "由于您设置了切分字段, 将暂时不给你采取[%s]措施",writeMode));
                assChar=true;
            }
            if(assChar==false) {
                // truncate option handler
                // truncate option handler
                if ("truncate".equals(writeMode)) {
                    LOG.info(String.format(
                            "由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容",
                            path, fileName));
                    Set<String> fullFileNameToDelete = new HashSet<String>();
                    for (String each : allFileExists) {
                        fullFileNameToDelete.add(UnstructuredStorageWriterUtil
                                .buildFilePath(path, each, null));
                    }
                    LOG.info(String.format(
                            "删除目录path:[%s] 下指定前缀fileName:[%s] 文件列表如下: [%s]", path,
                            fileName,
                            StringUtils.join(fullFileNameToDelete.iterator(), ", ")));

                    this.ftpHelper.deleteFiles(fullFileNameToDelete);
                } else if ("append".equals(writeMode)) {
                    LOG.info(String
                            .format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
                                    path, fileName));
                    LOG.info(String.format(
                            "目录path:[%s] 下已经存在的指定前缀fileName:[%s] 文件列表如下: [%s]",
                            path, fileName,
                            StringUtils.join(allFileExists.iterator(), ", ")));
                } else if ("nonConflict".equals(writeMode)) {
                    LOG.info(String.format(
                            "由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));
                    if (!allFileExists.isEmpty()) {
                        LOG.info(String.format(
                                "目录path:[%s] 下指定前缀fileName:[%s] 冲突文件列表如下: [%s]",
                                path, fileName,
                                StringUtils.join(allFileExists.iterator(), ", ")));
                        throw DataXException
                                .asDataXException(
                                        FtpWriterErrorCode.ILLEGAL_VALUE,
                                        String.format(
                                                "您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.",
                                                path));
                    }
                } else {
                    throw DataXException
                            .asDataXException(
                                    FtpWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                            writeMode));
                }
            }
        }

        @Override
        public void post() {
            for(Map.Entry<String,UnstructuredWriterEx> map: new Task().getAssCharMap().entrySet()) {
                            LOG.info("Map:"+map.getKey()+","+map.getValue());
                            try {
                                IOUtils.closeQuietly(map.getValue().getWriter());
                                IOUtils.closeQuietly(map.getValue().getOutputStream());
                            } catch (Exception e) {
                                LOG.error("close Exception",e.getMessage());
                                e.printStackTrace();
                            }
                            LOG.info("依次关闭写入流循环中");
                        }
                LOG.info("关闭写入流完毕");
        }

        @Override
        public void destroy() {
            try {
                this.ftpHelper.logoutFtpServer();
            } catch (Exception e) {
                String message = String
                        .format("关闭与ftp服务器连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                                host, username, port, e.getMessage());
                LOG.error(message, e);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return UnstructuredStorageWriterUtil.split(this.writerSliceConfig,
                    this.allFileExists, mandatoryNumber);
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String path;
        private String fileName;
        private String suffix;

        private String protocol;
        private String host;
        private int port;
        private String username;
        private String password;
        private int timeout;
        Map<String, UnstructuredWriterEx> assCharMap= new HashMap<String, UnstructuredWriterEx>();


        private IFtpHelper ftpHelper = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(Key.PATH);
            this.fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
            this.suffix = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.SUFFIX);

            this.host = this.writerSliceConfig.getString(Key.HOST);
            this.port = this.writerSliceConfig.getInt(Key.PORT);
            this.username = this.writerSliceConfig.getString(Key.USERNAME);
            this.password = this.writerSliceConfig.getString(Key.PASSWORD);
            this.timeout = this.writerSliceConfig.getInt(Key.TIMEOUT,
                    Constant.DEFAULT_TIMEOUT);
            this.protocol = this.writerSliceConfig.getString(Key.PROTOCOL);

            if ("sftp".equalsIgnoreCase(this.protocol)) {
                this.ftpHelper = new SftpHelperImpl();
            } else if ("ftp".equalsIgnoreCase(this.protocol)) {
                this.ftpHelper = new StandardFtpHelperImpl();
            }
            try {
                RetryUtil.executeWithRetry(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        ftpHelper.loginFtpServer(host, username, password,
                                port, timeout);
                        return null;
                    }
                }, 3, 4000, true);
            } catch (Exception e) {
                String message = String
                        .format("与ftp服务器建立连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                                host, username, port, e.getMessage());
                LOG.error(message);
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.FAIL_LOGIN, message, e);
            }
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            if(assChar==true){
                try {
                    String nullFormat = writerSliceConfig.getString(Key.NULL_FORMAT);

                    // 兼容format & dataFormat
                    String dateFormat = writerSliceConfig.getString(Key.DATE_FORMAT);
                    DateFormat dateParse = null; // warn: 可能不兼容
                    if (StringUtils.isNotBlank(dateFormat)) {
                        dateParse = new SimpleDateFormat(dateFormat);
                    }

                    String index = null;
//                    int firstIndex = this.fileName.indexOf(":");
//                    int lastIndex = this.fileName.lastIndexOf(":");
//                    if (firstIndex >= 0 && lastIndex > 0) {
//                        index = this.fileName.substring(firstIndex + 1, lastIndex);
//                    }
                    Pattern pattern = Pattern.compile(":\\d+:");
                    Matcher matcher=pattern.matcher(this.fileName);
                    while(matcher.find()){
                        index= this.fileName.substring(matcher.start()+1,matcher.end()-1);
                    }

                    LOG.info("切分字段index:" + index);
                    if (index == null ) {
                        throw DataXException.asDataXException(
                                UnstructuredStorageWriterErrorCode.ILLEGAL_VALUE,
                                String.format("获取列下标异常,你配置的列下标为%s", index));
                    }

                    Record record = null;
                    UnstructuredWriter unstructuredWriter;
                    OutputStream outputStream = null;
                    BufferedWriter writer = null;
                    UnstructuredWriterEx unstructuredWriterEx=null;
                    try {
                        while ((record = lineReceiver.getFromReader()) != null) {
                            String value = record.getColumn(Integer.parseInt(index)).getRawData().toString();
                            if (value == null ) {
                               value="";
                            }
                            unstructuredWriterEx = assCharMap.get(value);
                            if (unstructuredWriterEx == null || unstructuredWriterEx.getUnstructuredWriter()==null) {
                                String fileName = this.fileName.replace(":" + index + ":", "_" + value);//已经生成随机码好的文件名
                                String preName2 = prefuName.replace(":" + index + ":", "_" + value);//以此开头的文件名
                                String path=this.path;
                                outputStream = initOutPutStream(fileName,path, preName2);
                                writer = initBufferedWriter(outputStream);
                                unstructuredWriter = initUnstructuredWriter(writer);
                                unstructuredWriterEx=new UnstructuredWriterEx(outputStream,writer,unstructuredWriter);
                                assCharMap.put(value, unstructuredWriterEx);
                                LOG.info("新建切分文件：" + fileName);
                            }
                            UnstructuredStorageWriterUtil.transportOneRecord(record,
                                    nullFormat, dateParse, this.getTaskPluginCollector(),
                                    unstructuredWriterEx.getUnstructuredWriter());
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        throw DataXException.asDataXException(
                                FtpWriterErrorCode.WRITE_FILE_IO_ERROR,
                                "无法创建待写文件");
                    }finally {
//                        LOG.info("依次关闭写入流:"+assCharMap.size());
//                        if(writer!=null) {
//                            writer.close();
//                        }
//                        if(outputStream!=null) {
//                            outputStream.close();
//                        }
//                        for(Map.Entry<String,UnstructuredWriterEx> map:assCharMap.entrySet()) {
//                            LOG.info("Map:"+map.getKey()+","+map.getValue());
//                            try {
////                                map.getValue().getUnstructuredWriter().flush();
////                                map.getValue().getUnstructuredWriter().close();
//                                IOUtils.closeQuietly(map.getValue().getUnstructuredWriter());
//                                LOG.info("关闭Writer"+map.getValue().getWriter());
//                                IOUtils.closeQuietly(map.getValue().getWriter());
////                                map.getValue().getWriter().flush();
////                                map.getValue().getWriter().close();
//                                LOG.info("关闭OutputStream"+map.getValue().getOutputStream());
////                                 map.getValue().getOutputStream().flush();
//                                try {
//                                    IOUtils.closeQuietly(map.getValue().getOutputStream());
//                                } catch (Exception e) {
//                                    LOG.error("close Exception",e.getMessage());
//                                    e.printStackTrace();
//                                }
//                            } catch (Exception e) {
//                                LOG.error("close Exception",e.getMessage());
//                                e.printStackTrace();
//                            }
//                            LOG.info("依次关闭写入流循环中");
//                        }
                        LOG.info("关闭写入流完毕");
                    }
                }catch (Exception e) {
                    e.printStackTrace();
                    throw DataXException.asDataXException(
                            FtpWriterErrorCode.WRITE_FILE_IO_ERROR,
                            "无法创建待写文件 ");
                }
            }else {
                String fileFullPath = UnstructuredStorageWriterUtil.buildFilePath(
                        this.path, this.fileName, this.suffix);
                LOG.info(String.format("write to file : [%s]", fileFullPath));

                OutputStream outputStream = null;
                try {
                    outputStream = this.ftpHelper.getOutputStream(fileFullPath);
                    UnstructuredStorageWriterUtil.writeToStream(lineReceiver,
                            outputStream, this.writerSliceConfig, this.fileName,
                            this.getTaskPluginCollector());
                } catch (Exception e) {
                    throw DataXException.asDataXException(
                            FtpWriterErrorCode.WRITE_FILE_IO_ERROR,
                            String.format("无法创建待写文件 : [%s]", this.fileName), e);
                } finally {
                    IOUtils.closeQuietly(outputStream);
                }
                LOG.info("end do write");
            }
        }

        public Map<String,UnstructuredWriterEx> getAssCharMap(){
            return assCharMap;
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {
            try {
                this.ftpHelper.logoutFtpServer();
            } catch (Exception e) {
                String message = String
                        .format("关闭与ftp服务器连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                                host, username, port, e.getMessage());
                LOG.error(message, e);
            }
        }

        public   OutputStream initOutPutStream(String fileName,String path,String preName){
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            Set<String> allFileExists = new HashSet<String>();
            Set<String> fullFileNameToDelete = new HashSet<String>();

            //在流还未关闭的时候重新new ftp/sftp工具类，否则将会报错（无法创建代写文件） --pwj 2018.11.16
            this.protocol = this.writerSliceConfig.getNecessaryValue(
                    Key.PROTOCOL, FtpWriterErrorCode.REQUIRED_VALUE);
                this.host = this.writerSliceConfig.getNecessaryValue(Key.HOST,
                        FtpWriterErrorCode.REQUIRED_VALUE);
                this.username = this.writerSliceConfig.getNecessaryValue(
                        Key.USERNAME, FtpWriterErrorCode.REQUIRED_VALUE);
                this.password = this.writerSliceConfig.getNecessaryValue(
                        Key.PASSWORD, FtpWriterErrorCode.REQUIRED_VALUE);
                this.timeout = this.writerSliceConfig.getInt(Key.TIMEOUT,
                        Constant.DEFAULT_TIMEOUT);
                this.port = this.writerSliceConfig.getInt(Key.PORT,
                            Constant.DEFAULT_FTP_PORT);
            if ("sftp".equalsIgnoreCase(this.protocol)) {
                this.port = this.writerSliceConfig.getInt(Key.PORT,
                        Constant.DEFAULT_SFTP_PORT);
                this.ftpHelper = new SftpHelperImpl();
            } else if ("ftp".equalsIgnoreCase(this.protocol)) {
                this.port = this.writerSliceConfig.getInt(Key.PORT,
                        Constant.DEFAULT_FTP_PORT);
                this.ftpHelper = new StandardFtpHelperImpl();
            } else {
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.ILLEGAL_VALUE, String.format(
                                "仅支持 ftp和sftp 传输协议 , 不支持您配置的传输协议: [%s]",
                                protocol));
            }
                try {
                    RetryUtil.executeWithRetry(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            ftpHelper.loginFtpServer(host, username, password,
                                    port, timeout);
                            return null;
                        }
                    }, 3, 4000, true);
                } catch (Exception e) {
                    String message = String
                            .format("与ftp服务器建立连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                                    host, username, port, e.getMessage());
                    LOG.error(message);
                    throw DataXException.asDataXException(
                            FtpWriterErrorCode.FAIL_LOGIN, message, e);
                }

            //sftp清理路径下所有文件
                LOG.info("SFTP获取所有路径下文件");
                try {
                    allFileExists=ftpHelper.getAllFilesInDir(path,preName);
                } catch (Exception e) {
                    String message = String
                            .format("获取path:[%s] 下文件列表时发生I/O异常,请确认与ftp服务器的连接正常,拥有目录ls权限, errorMessage:%s",
                                    path, e.getMessage());
                    LOG.error(message);
                }
            if ("truncate".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容",
                        path, preName));
                for (String each : allFileExists) {
                    fullFileNameToDelete.add(UnstructuredStorageWriterUtil
                            .buildFilePath(path, each, null));
                }
                LOG.info(String.format(
                        "删除目录path:[%s] 下指定前缀fileName:[%s] 文件列表如下: [%s]", path,
                        preName,
                        StringUtils.join(fullFileNameToDelete.iterator(), ", ")));

                this.ftpHelper.deleteFiles(fullFileNameToDelete);
            } else if ("append".equals(writeMode)) {
                LOG.info(String
                        .format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
                                path, preName));
                LOG.info(String.format(
                        "目录path:[%s] 下已经存在的指定前缀fileName:[%s] 文件列表如下: [%s]",
                        path, preName,
                        StringUtils.join(fullFileNameToDelete.iterator(), ", ")));
            } else if ("nonConflict".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));
                if (!fullFileNameToDelete.isEmpty()) {
                    LOG.info(String.format(
                            "目录path:[%s] 下指定前缀fileName:[%s] 冲突文件列表如下: [%s]",
                            path, preName,
                            StringUtils.join(fullFileNameToDelete.iterator(), ", ")));
                    throw DataXException
                            .asDataXException(
                                    FtpWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.",
                                            path));
                }
            } else {
                throw DataXException
                        .asDataXException(
                                FtpWriterErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                        writeMode));
            }
            String fileFullPath=UnstructuredStorageWriterUtil.buildFilePath(path,fileName,this.suffix);
            OutputStream outputStream = null;
            try {
                outputStream = this.ftpHelper.getOutputStream(fileFullPath);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.WRITE_FILE_IO_ERROR,
                        String.format("无法创建待写文件 : [%s]", this.fileName), e);
            }
            return outputStream;
        }

        public  BufferedWriter initBufferedWriter(OutputStream outputStream) {
            String compress = this.writerSliceConfig.getString(Key.COMPRESS);
            String encoding = this.writerSliceConfig.getString(Key.ENCODING,
                    Constant.DEFAULT_ENCODING);

            if (StringUtils.isBlank(encoding)) {
                LOG.warn(String.format("您配置的encoding为[%s], 使用默认值[%s]", encoding,
                        Constant.DEFAULT_ENCODING));
                encoding = Constant.DEFAULT_ENCODING;
            }
            //生成BufferedWriter
            BufferedWriter writer = null;
            // compress logic
            try {
                if (null == compress) {
                    writer = new BufferedWriter(new OutputStreamWriter(
                            outputStream, encoding));
                } else {
                    // TODO more compress
                    if ("gzip".equalsIgnoreCase(compress)) {
                        CompressorOutputStream compressorOutputStream = new GzipCompressorOutputStream(
                                outputStream);
                        writer = new BufferedWriter(new OutputStreamWriter(
                                compressorOutputStream, encoding));
                    } else if ("bzip2".equalsIgnoreCase(compress)) {
                        CompressorOutputStream compressorOutputStream = new BZip2CompressorOutputStream(
                                outputStream);
                        writer = new BufferedWriter(new OutputStreamWriter(
                                compressorOutputStream, encoding));
                    } else {
                        throw DataXException
                                .asDataXException(
                                        FtpWriterErrorCode.ILLEGAL_VALUE,
                                        String.format(
                                                "仅支持 gzip, bzip2 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]",
                                                compress));
                    }
                }
            }catch (Exception e) {
                e.printStackTrace();
                throw DataXException
                        .asDataXException(
                                FtpWriterErrorCode.Write_FILE_WITH_CHARSET_ERROR,
                                "生成写入文件失败！");
            }
            LOG.info("生成写入Writer文件成功！");
            return writer;
        }

        public  UnstructuredWriter initUnstructuredWriter(BufferedWriter writer){
            UnstructuredWriter unstructuredWriter=null;

            String fileFormat = writerSliceConfig.getString(Key.FILE_FORMAT,
                    Constant.FILE_FORMAT_TEXT);

            String delimiterInStr = writerSliceConfig.getString(Key.FIELD_DELIMITER);
            if (null != delimiterInStr && 1 != delimiterInStr.length()) {
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.ILLEGAL_VALUE,
                        String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
            }
            if (null == delimiterInStr) {
                LOG.warn(String.format("您没有配置列分隔符, 使用默认值[%s]",
                        Constant.DEFAULT_FIELD_DELIMITER));
            }
            // warn: fieldDelimiter could not be '' for no fieldDelimiter
            char fieldDelimiter = writerSliceConfig.getChar(Key.FIELD_DELIMITER,
                    Constant.DEFAULT_FIELD_DELIMITER);
            try {
                unstructuredWriter = TextCsvWriterManager
                        .produceUnstructuredWriter(fileFormat, fieldDelimiter, writer);

                List<String> headers = writerSliceConfig.getList(Key.HEADER, String.class);
                if (null != headers && !headers.isEmpty()) {
                    unstructuredWriter.writeOneRecord(headers);
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw DataXException
                        .asDataXException(
                                UnstructuredStorageWriterErrorCode.Write_FILE_WITH_CHARSET_ERROR,
                                "生成写入文件失败！");
            }
            LOG.info("生成写入文件成功！");
            return unstructuredWriter;
        }

    }

    public static class  UnstructuredWriterEx{
        OutputStream outputStream;
        BufferedWriter writer;
        UnstructuredWriter unstructuredWriter;

        public UnstructuredWriterEx(OutputStream outputStream, BufferedWriter writer, UnstructuredWriter unstructuredWriter) {
            this.outputStream = outputStream;
            this.writer = writer;
            this.unstructuredWriter = unstructuredWriter;
        }

        public OutputStream getOutputStream() {
            return outputStream;
        }

        public void setOutputStream(OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        public BufferedWriter getWriter() {
            return writer;
        }

        public void setWriter(BufferedWriter writer) {
            this.writer = writer;
        }

        public UnstructuredWriter getUnstructuredWriter() {
            return unstructuredWriter;
        }

        public void setUnstructuredWriter(UnstructuredWriter unstructuredWriter) {
            this.unstructuredWriter = unstructuredWriter;
        }
    }
}
