package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public  class HdfsHelper {
    public static final Logger LOG = LoggerFactory.getLogger(HdfsWriter.Job.class);

    public static class BufferWriterBatchForPart
    {
        private VectorizedRowBatch batch;
        private org.apache.orc.Writer writer;
        private String fileName;

        public VectorizedRowBatch getBatch() {
            return batch;
        }

        public org.apache.orc.Writer getWriter() {
            return writer;
        }

        public void setBatch(VectorizedRowBatch batch) {
            this.batch = batch;
        }

        public void setWriter(org.apache.orc.Writer writer) {
            this.writer = writer;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }
    }

    public FileSystem fileSystem = null;
    public JobConf conf = null;
    public org.apache.hadoop.conf.Configuration hadoopConf = null;
    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";

    private Map<String,BufferWriterBatchForPart> writerBatchMap=new HashMap();

    // Kerberos
    private Boolean haveKerberos = false;
    private String  kerberosKeytabFilePath;
    private String  kerberosPrincipal;

    public void getFileSystem(String defaultFS, Configuration taskConfig){
        hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);

        //设置HDFS HA配置
        String hdfsHAConf=taskConfig.getString(Key.HDFS_HA_CONF);
        if(hdfsHAConf!=null && !hdfsHAConf.trim().equals(""))
        {
            JSONObject hdfsHAConfJson=JSONObject.parseObject(hdfsHAConf);
            LOG.info(String.format("您设置了HDFS的HA连接方式: %s", hdfsHAConf));
            for(Map.Entry<String,Object> entry : hdfsHAConfJson.entrySet())
            {
                hadoopConf.set(entry.getKey(), (String)entry.getValue());
            }
        }

        //是否有Kerberos认证
        this.haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if(haveKerberos){
            this.kerberosKeytabFilePath = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            this.kerberosPrincipal = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);
        conf = new JobConf(hadoopConf);
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            String message = String.format("获取FileSystem时发生网络IO异常,请检查您的网络是否正常!HDFS地址：[%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }catch (Exception e) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }

        if(null == fileSystem || null == conf){
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath){
        if(haveKerberos && StringUtils.isNotBlank(this.kerberosPrincipal) && StringUtils.isNotBlank(this.kerberosKeytabFilePath)){
            UserGroupInformation.setConfiguration(this.hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            } catch (Exception e) {
                String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                        kerberosKeytabFilePath, kerberosPrincipal);
                LOG.error(message);
                throw DataXException.asDataXException(HdfsWriterErrorCode.KERBEROS_LOGIN_ERROR, e);
            }
        }
    }

    /**
     *获取指定目录先的文件列表
     * @param dir
     * @return
     * 拿到的是文件全路径，
     * eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile
     */
    public String[] hdfsDirList(String dir){
        Path path = new Path(dir);
        String[] files = null;
        try {
            FileStatus[] status = fileSystem.listStatus(path);
            files = new String[status.length];
            for(int i=0;i<status.length;i++){
                files[i] = status[i].getPath().toString();
            }
        } catch (IOException e) {
            String message = String.format("获取目录[%s]文件列表时发生网络IO异常,请检查您的网络是否正常！", dir);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    /**
     * 获取以fileName__ 开头的文件列表
     * @param dir
     * @param fileName
     * @return
     */
    public Path[] hdfsDirList(String dir,String fileName){
        Path path = new Path(dir);
        Path[] files = null;
        String filterFileName = fileName + "__*";
        try {
            PathFilter pathFilter = new GlobFilter(filterFileName);
            FileStatus[] status = fileSystem.listStatus(path,pathFilter);
            files = new Path[status.length];
            for(int i=0;i<status.length;i++){
                files[i] = status[i].getPath();
            }
        } catch (IOException e) {
            String message = String.format("获取目录[%s]下文件名以[%s]开头的文件列表时发生网络IO异常,请检查您的网络是否正常！",
                    dir,fileName);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    public boolean isPathexists(String filePath) {
        Path path = new Path(filePath);
        boolean exist = false;
        try {
            exist = fileSystem.exists(path);
        } catch (IOException e) {
            String message = String.format("判断文件路径[%s]是否存在时发生网络IO异常,请检查您的网络是否正常！",
                    "message:filePath =" + filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return exist;
    }

    public boolean isPathDir(String filePath) {
        Path path = new Path(filePath);
        boolean isDir = false;
        try {
            isDir = fileSystem.isDirectory(path);
        } catch (IOException e) {
            String message = String.format("判断路径[%s]是否是目录时发生网络IO异常,请检查您的网络是否正常！", filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return isDir;
    }

    public void deleteFiles(Path[] paths){
        for(int i=0;i<paths.length;i++){
            LOG.info(String.format("delete file [%s].", paths[i].toString()));
            try {
                fileSystem.delete(paths[i],true);
            } catch (IOException e) {
                String message = String.format("删除文件[%s]时发生IO异常,请检查您的网络是否正常！",
                        paths[i].toString());
                LOG.error(message);
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }
        }
    }

    public void deleteDir(Path path){
        LOG.info(String.format("start delete tmp dir [%s] .",path.toString()));
        try {
            if(isPathexists(path.toString())) {
                fileSystem.delete(path, true);
            }
        } catch (Exception e) {
            String message = String.format("删除临时目录[%s]时发生IO异常,请检查您的网络是否正常！", path.toString());
            LOG.error(message,e);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        LOG.info(String.format("finish delete tmp dir [%s] .",path.toString()));
    }

    public void moveTo(String src,String dst){
        Path srcPath=new Path(src);
        try {

            //遍历
            FileStatus[] fileStatus=this.fileSystem.listStatus(srcPath);
            for(FileStatus fileSta:fileStatus){

                String[] strs=fileSta.getPath().toString().split(IOUtils.DIR_SEPARATOR+"");
                String lastName=strs[strs.length-1];

                //如果是目录则进一步move
                if(this.fileSystem.isDirectory(fileSta.getPath()))
                {
                    this.fileSystem.mkdirs(new Path(dst+IOUtils.DIR_SEPARATOR+lastName));
                    moveTo(fileSta.getPath().toString(),dst+IOUtils.DIR_SEPARATOR+lastName);
                }
                //如果是文件,则rename
                else
                {
                    HashSet<String> srcFiles=new HashSet<String>();
                    srcFiles.add(fileSta.getPath().toString());
                    HashSet<String> dstFiles=new HashSet<String>();
                    dstFiles.add(dst+IOUtils.DIR_SEPARATOR+lastName);
                    renameFile(srcFiles,dstFiles);
                }

            }

            /*String[] strs=src.split(IOUtils.DIR_SEPARATOR+"");
            String lastName=strs[strs.length-1];
            if(this.fileSystem.isDirectory(srcPath)){
                this.fileSystem.mkdirs(new Path(dst+IOUtils.DIR_SEPARATOR+lastName));

                //遍历
                FileStatus[] fileStatus=this.fileSystem.listStatus(srcPath);
                for(FileStatus fileSta:fileStatus){
                    copyAllTo(fileSta.getPath().toString(),dst+IOUtils.DIR_SEPARATOR+lastName);
                }

            }else{
                this.fileSystem.mkdirs(new Path(dst));
                HashSet<String> srcFiles=new HashSet<String>();
                srcFiles.add(src);
                HashSet<String> dstFiles=new HashSet<String>();
                dstFiles.add(dst+IOUtils.DIR_SEPARATOR+lastName);
                renameFile(srcFiles,dstFiles);
            }*/
        }
        catch(Exception e)
        {
            String message = String.format("rename目录的时候发生异常,请联系管理员！");
            LOG.error(message,e);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        finally {
            deleteDir(srcPath);
        }
    }

    public void renameFile(HashSet<String> tmpFiles, HashSet<String> endFiles){
        Path tmpFilesParent = null;
        if(tmpFiles.size() != endFiles.size()){
            String message = String.format("临时目录下文件名个数与目标文件名个数不一致!");
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
        }else{
            try{
                for (Iterator it1=tmpFiles.iterator(),it2=endFiles.iterator();it1.hasNext()&&it2.hasNext();){
                    String srcFile = it1.next().toString();
                    String dstFile = it2.next().toString();
                    Path srcFilePah = new Path(srcFile);
                    Path dstFilePah = new Path(dstFile);
                    if(tmpFilesParent == null){
                        tmpFilesParent = srcFilePah.getParent();
                    }

                    if(fileSystem.exists(srcFilePah))
                    {
                        LOG.info(String.format("start rename file [%s] to file [%s].", srcFile,dstFile));
                        boolean renameTag = false;
                        long fileLen = fileSystem.getFileStatus(srcFilePah).getLen();
                        if(fileLen>0){
                            renameTag = fileSystem.rename(srcFilePah,dstFilePah);
                            if(!renameTag){
                                String message = String.format("重命名文件[%s]失败,请检查您的网络是否正常！", srcFile);
                                LOG.error(message);
                                throw DataXException.asDataXException(HdfsWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
                            }
                            LOG.info(String.format("finish rename file [%s] to file [%s].", srcFile,dstFile));
                        }else{
                            LOG.info(String.format("文件［%s］内容为空,请检查写入是否正常！", srcFile));
                        }
                    }
                    else
                    {
                        LOG.info(String.format("File [%s] not exists.", srcFile));
                    }
                }
            }catch (Exception e) {
                String message = String.format("重命名文件时发生异常,请检查您的网络是否正常！");
                LOG.error(message,e);
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }finally {
                deleteDir(tmpFilesParent);
            }
        }
    }

    //关闭FileSystem
    public void closeFileSystem(){
        try {
            fileSystem.close();
        } catch (IOException e) {
            String message = String.format("关闭FileSystem时发生IO异常,请检查您的网络是否正常！");
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }


    //textfile格式文件
    public  FSDataOutputStream getOutputStream(String path){
        Path storePath = new Path(path);
        FSDataOutputStream fSDataOutputStream = null;
        try {
            fSDataOutputStream = fileSystem.create(storePath);
        } catch (IOException e) {
            String message = String.format("Create an FSDataOutputStream at the indicated Path[%s] failed: [%s]",
                    "message:path =" + path);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
        return fSDataOutputStream;
    }

    /**
     * 写textfile类型文件
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    public void textFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                   TaskPluginCollector taskPluginCollector){
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER);
        List<Configuration>  columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS,null);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        String attempt = "attempt_"+dateFormat.format(new Date())+"_0001_m_000000_0";
        Path outputPath = new Path(fileName);
        //todo 需要进一步确定TASK_ATTEMPT_ID
        conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
        FileOutputFormat outFormat = new TextOutputFormat();
        outFormat.setOutputPath(conf, outputPath);
        outFormat.setWorkOutputPath(conf, outputPath);
        if(null != compress) {
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                outFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        try {
            RecordWriter writer = outFormat.getRecordWriter(fileSystem, conf, outputPath.toString(), Reporter.NULL);
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<Text, Boolean> transportResult = transportOneRecord(record, fieldDelimiter, columns, taskPluginCollector);
                if (!transportResult.getRight()) {
                    writer.write(NullWritable.get(),transportResult.getLeft());
                }
            }
            writer.close(Reporter.NULL);
        } catch (Exception e) {
            String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
            LOG.error(message);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    public static MutablePair<Text, Boolean> transportOneRecord(
            Record record, char fieldDelimiter, List<Configuration> columnsConfiguration, TaskPluginCollector taskPluginCollector) {
        MutablePair<List<Object>, Boolean> transportResultList =  transportOneRecord(record,columnsConfiguration,taskPluginCollector);
        //保存<转换后的数据,是否是脏数据>
        MutablePair<Text, Boolean> transportResult = new MutablePair<Text, Boolean>();
        transportResult.setRight(false);
        if(null != transportResultList){
            Text recordResult = new Text(StringUtils.join(transportResultList.getLeft(), fieldDelimiter));
            transportResult.setRight(transportResultList.getRight());
            transportResult.setLeft(recordResult);
        }
        return transportResult;
    }

    public Class<? extends CompressionCodec>  getCompressCodec(String compress){
        Class<? extends CompressionCodec> codecClass = null;
        if(null == compress){
            codecClass = null;
        }else if("GZIP".equalsIgnoreCase(compress)){
            codecClass = org.apache.hadoop.io.compress.GzipCodec.class;
        }else if ("BZIP2".equalsIgnoreCase(compress)) {
            codecClass = org.apache.hadoop.io.compress.BZip2Codec.class;
        }else if("SNAPPY".equalsIgnoreCase(compress)){
            //todo 等需求明确后支持 需要用户安装SnappyCodec
            codecClass = org.apache.hadoop.io.compress.SnappyCodec.class;
            // org.apache.hadoop.hive.ql.io.orc.ZlibCodec.class  not public
            //codecClass = org.apache.hadoop.hive.ql.io.orc.ZlibCodec.class;
        }else {
            throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                    String.format("目前不支持您配置的 compress 模式 : [%s]", compress));
        }
        return codecClass;
    }

    /**
     * 写orcfile类型文件
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    public void orcFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                  TaskPluginCollector taskPluginCollector){
        List<Configuration>  columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, null);
        List<String> columnNames = getColumnNames(columns);
        List<ObjectInspector> columnTypeInspectors = getColumnTypeInspectors(columns);
        StructObjectInspector inspector = (StructObjectInspector)ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, columnTypeInspectors);

        OrcSerde orcSerde = new OrcSerde();

        FileOutputFormat outFormat = new OrcOutputFormat();
        if(!"NONE".equalsIgnoreCase(compress) && null != compress ) {
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                outFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        try {
            RecordWriter writer = outFormat.getRecordWriter(fileSystem, conf, fileName, Reporter.NULL);
            Record record = null;

            /*long startNextTime=System.currentTimeMillis();
            long nextTimeSum=0;
            long startWriteTime=0;
            long writeTimeSum=0;
            long startSerTime=0;
            long serTimeSum=0;
            long serBytesSum=0;

            int cnt=1;
            int writeCnt=1;*/

            while ((record = lineReceiver.getFromReader()) != null) {

                /*nextTimeSum+=(System.currentTimeMillis()-startNextTime);
                if(cnt>=10000)
                {
                    LOG.info("getFromReader平均耗时: "+(1.0*nextTimeSum/10000.0));
                    nextTimeSum=0;
                    cnt=0;
                }*/

                MutablePair<List<Object>, Boolean> transportResult =  transportOneRecord(record,columns,taskPluginCollector);

                if (!transportResult.getRight()) {

                    //startSerTime=System.currentTimeMillis();

                    //serBytesSum+=record.getByteSize();

                    Object datObj=orcSerde.serialize(transportResult.getLeft(), inspector);

                    /*serTimeSum+=System.currentTimeMillis()-startSerTime;
                    if(writeCnt>=10000)
                    {
                        LOG.info("serialize平均耗时: "+(1.0*serTimeSum/10000.0)+",平均字节数: "+(1.0*serBytesSum/10000.0));
                        serTimeSum=0;
                        serBytesSum=0;
                    }

                    startWriteTime=System.currentTimeMillis();*/

                    writer.write(NullWritable.get(), datObj);

                    /*writeTimeSum+=System.currentTimeMillis()-startWriteTime;
                    if(writeCnt>=10000)
                    {
                        LOG.info("write平均耗时: "+(1.0*writeTimeSum/10000.0));
                        writeTimeSum=0;
                        writeCnt=0;
                    }

                    writeCnt++;*/
                }

                //startNextTime=System.currentTimeMillis();
                //cnt++;

            }
            writer.close(Reporter.NULL);
        } catch (Exception e) {
            String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
            LOG.error(message);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    private TypeDescription createOrcSchema(List<Integer> colIndexes,List<Configuration> columns)
    {
        TypeDescription schema = TypeDescription.createStruct();

        for (int i=0; i<columns.size(); i++) {
            //如果是用于subDir的column，则不用写入到文件
            if(colIndexes!=null && !colIndexes.isEmpty() && colIndexes.contains(i))
            {
                continue;
            }
            else
            {
                Configuration eachColumnConf=columns.get(i);
                String columnName=eachColumnConf.getString(Key.NAME);
                String columnTypeStr=eachColumnConf.getString(Key.TYPE);
                SupportHiveDataType columnType = SupportHiveDataType.valueOf(columnTypeStr.toUpperCase());
                switch (columnType) {
                    case TINYINT:
                        schema.addField(columnName,TypeDescription.createInt());
                        break;
                    case SMALLINT:
                        schema.addField(columnName,TypeDescription.createInt());
                        break;
                    case INT:
                        schema.addField(columnName,TypeDescription.createInt());
                        break;
                    case BIGINT:
                        schema.addField(columnName,TypeDescription.createLong());
                        break;
                    case FLOAT:
                        schema.addField(columnName,TypeDescription.createFloat());
                        break;
                    case DOUBLE:
                        schema.addField(columnName,TypeDescription.createDouble());
                        break;
                    case STRING:
                        schema.addField(columnName,TypeDescription.createString());
                        break;
                    case VARCHAR:
                        schema.addField(columnName,TypeDescription.createVarchar());
                        break;
                    case CHAR:
                        schema.addField(columnName,TypeDescription.createChar());
                        break;
                    case BOOLEAN:
                        schema.addField(columnName,TypeDescription.createBoolean());
                        break;
                    case DATE:
                        schema.addField(columnName,TypeDescription.createDate());
                        break;
                    case TIMESTAMP:
                        schema.addField(columnName,TypeDescription.createTimestamp());
                        break;
                    default:
                        throw DataXException
                                .asDataXException(
                                        HdfsWriterErrorCode.ILLEGAL_VALUE,
                                        String.format(
                                                "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                                columnName,
                                                columnTypeStr));
                }
            }
        }

        return schema;
    }

    private org.apache.orc.Writer createOrcWriter(String fileName, org.apache.hadoop.conf.Configuration conf, TypeDescription schema,Configuration config)
    {
        String compress = config.getString(Key.COMPRESS, "NONE");
        long stripeSize=config.getLong(Key.ORC_STRIPE_SIZE,67108864);
        int bufferSize=config.getInt(Key.ORC_BUFFER_SIZE,262144);
        long blockSize=config.getLong(Key.ORC_BLOCK_SIZE,134217728);
        String orcVersionStr=config.getString(Key.ORC_VERSION,"V_0_12");
        OrcFile.Version orcVersion=null;
        if(orcVersionStr!=null && orcVersionStr.trim().equalsIgnoreCase("V_0_11"))
        {
            orcVersion=OrcFile.Version.V_0_11;
        }
        else
        {
            orcVersion=OrcFile.Version.V_0_12;
        }

        Properties tblPorp=new Properties();
        //tblPorp.setProperty("orc.compress.size","262144");

        org.apache.orc.Writer writer = null;

        OrcFile.WriterOptions writerOptions=null;

        LOG.info("ORC相关配置: stripeSize: "+stripeSize+", bufferSize: "+bufferSize+", blockSize: "+blockSize+", orcVersion: "+(orcVersion==OrcFile.Version.V_0_11?"V_0_11":"V_0_12"));

        if("NONE".equalsIgnoreCase(compress)){
            LOG.info("未启用压缩.");
            writerOptions=OrcFile.writerOptions(tblPorp,conf)
                    .setSchema(schema)
                    .stripeSize(stripeSize)
                    .bufferSize(bufferSize)
                    .blockSize(blockSize)
                    .compress(CompressionKind.NONE)
                    .version(orcVersion);
        }else if("ZLIB".equalsIgnoreCase(compress)){
            LOG.info("启用ZLIB压缩.");
            writerOptions=OrcFile.writerOptions(tblPorp,conf)
                    .setSchema(schema)
                    .stripeSize(stripeSize)
                    .bufferSize(bufferSize)
                    .blockSize(blockSize)
                    .compress(CompressionKind.ZLIB)
                    .version(orcVersion);
        }else if ("LZO".equalsIgnoreCase(compress)) {
            LOG.info("启用LZO压缩.");
            writerOptions=OrcFile.writerOptions(tblPorp,conf)
                    .setSchema(schema)
                    .stripeSize(stripeSize)
                    .bufferSize(bufferSize)
                    .blockSize(blockSize)
                    .compress(CompressionKind.LZO)
                    .version(orcVersion);
        }else if("SNAPPY".equalsIgnoreCase(compress)){
            LOG.info("启用SNAPPY压缩.");
            writerOptions=OrcFile.writerOptions(tblPorp,conf)
                    .setSchema(schema)
                    .stripeSize(stripeSize)
                    .bufferSize(bufferSize)
                    .blockSize(blockSize)
                    .compress(CompressionKind.SNAPPY)
                    .version(orcVersion);
        }else {
            throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                    String.format("目前不支持您配置的 compress 模式 : [%s]", compress));
        }

        //Path path = new Path(fileName);
        //deleteDir(path);

        try
        {
            writer = OrcFile.createWriter(new Path(fileName),writerOptions);
        }
        catch(IOException e)
        {
            LOG.error("创建ORC Writer失败: ",e);
            throw DataXException
                    .asDataXException(
                            HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR,
                            String.format(
                                    "创建ORC Writer失败:[%s].",
                                    fileName));
        }

        return writer;
    }

    private void transportOrcBatch(VectorizedRowBatch batch,Record record,List<Configuration> columns,int rowCount,TaskPluginCollector taskPluginCollector,String encoding,List<Integer> colIndexes)
    {
        int recordLength = record.getColumnNumber();
        if (0 != recordLength) {
            Column column;
            int batchColIdx=0;
            for (int i = 0; i < recordLength; i++) {
                //如果是用于subDir的column，则不用写入到文件
                if(colIndexes!=null && !colIndexes.isEmpty() && colIndexes.contains(i))
                {
                    continue;
                }
                else
                {
                    column = record.getColumn(i);
                    //if (null != column.getRawData()) {
                    String rowData = column.getRawData()==null?null:column.getRawData().toString();
                    //Object rowData=column.getRawData();
                    Configuration columnConf=null;
                    try {
                        columnConf=columns.get(i);
                    }
                    catch(IndexOutOfBoundsException e)
                    {
                        LOG.error("源端字段的index超出目标端字段的数组下标,源端字段数量为 [{}], 目标端字段数量为 [{}], 请确保你配置的源端字段和目标端字段对应,源端字段最好不要使用*号.",recordLength,columns.size());
                        throw DataXException
                                .asDataXException(
                                        HdfsWriterErrorCode.ILLEGAL_VALUE,
                                        "源端字段的index超出目标端字段的数组下标,请确保你配置的源端字段和目标端字段对应,源端字段最好不要使用*号.");
                    }
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(columnConf.getString(Key.TYPE).toUpperCase());
                    //根据writer端类型配置做类型转换
                    try {
                        switch (columnType) {
                            case TINYINT:
                                //recordList.add(Byte.valueOf(rowData));
                                //((LongColumnVector) batch.cols[i]).vector[rowCount] = (rowData==null?null:Long.valueOf(rowData).longValue());
                                if(rowData==null)
                                {
                                    //((LongColumnVector) batch.cols[i]).fillWithNulls();
                                    batch.cols[batchColIdx].noNulls=false;
                                    batch.cols[batchColIdx].isNull[rowCount]=true;
                                }
                                else
                                {
                                    //((LongColumnVector) batch.cols[i]).fill(Long.valueOf(rowData).longValue());
                                    ((LongColumnVector) batch.cols[batchColIdx]).vector[rowCount]=Long.valueOf(rowData).longValue();
                                }
                                break;
                            case SMALLINT:
                                if(rowData==null)
                                {
                                    //((LongColumnVector) batch.cols[i]).fillWithNulls();
                                    batch.cols[batchColIdx].noNulls=false;
                                    batch.cols[batchColIdx].isNull[rowCount]=true;
                                }
                                else
                                {
                                    //((LongColumnVector) batch.cols[i]).fill(Long.valueOf(rowData).longValue());
                                    ((LongColumnVector) batch.cols[batchColIdx]).vector[rowCount]=Long.valueOf(rowData).longValue();
                                }
                                break;
                            case INT:
                                if(rowData==null)
                                {
                                    //((LongColumnVector) batch.cols[i]).fillWithNulls();
                                    batch.cols[batchColIdx].noNulls=false;
                                    batch.cols[batchColIdx].isNull[rowCount]=true;
                                }
                                else
                                {
                                    //((LongColumnVector) batch.cols[i]).fill(Long.valueOf(rowData).longValue());
                                    ((LongColumnVector) batch.cols[batchColIdx]).vector[rowCount]=Long.valueOf(rowData).longValue();
                                }
                                break;
                            case DATE:
                            case BIGINT:
                                if(rowData==null)
                                {
                                    //((LongColumnVector) batch.cols[i]).fillWithNulls();
                                    batch.cols[batchColIdx].noNulls=false;
                                    batch.cols[batchColIdx].isNull[rowCount]=true;
                                }
                                else
                                {
                                    //((LongColumnVector) batch.cols[i]).fill(Long.valueOf(rowData).longValue());
                                    ((LongColumnVector) batch.cols[batchColIdx]).vector[rowCount]=Long.valueOf(rowData).longValue();
                                }
                                break;
                            case FLOAT:
                                //recordList.add(Float.valueOf(rowData));
                                //((DoubleColumnVector) batch.cols[i]).vector[rowCount] = rowData==null?null:Double.valueOf(rowData).doubleValue();
                                if(rowData==null)
                                {
                                    //((DoubleColumnVector) batch.cols[i]).fillWithNulls();
                                    batch.cols[batchColIdx].noNulls=false;
                                    batch.cols[batchColIdx].isNull[rowCount]=true;
                                }
                                else
                                {
                                    //((DoubleColumnVector) batch.cols[i]).fill(Double.valueOf(rowData).doubleValue());
                                    ((DoubleColumnVector) batch.cols[batchColIdx]).vector[rowCount]=Double.valueOf(rowData).doubleValue();
                                }
                                break;
                            case DOUBLE:
                                if(rowData==null)
                                {
                                    //((DoubleColumnVector) batch.cols[i]).fillWithNulls();
                                    batch.cols[batchColIdx].noNulls=false;
                                    batch.cols[batchColIdx].isNull[rowCount]=true;
                                }
                                else
                                {
                                    //((DoubleColumnVector) batch.cols[i]).fill(Double.valueOf(rowData).doubleValue());
                                    ((DoubleColumnVector) batch.cols[batchColIdx]).vector[rowCount]=Double.valueOf(rowData).doubleValue();
                                }
                                break;
                            case STRING:
                            case VARCHAR:
                            case CHAR:
                                if(rowData==null)
                                {
                                    //((BytesColumnVector) batch.cols[i]).fillWithNulls();
                                    batch.cols[batchColIdx].noNulls=false;
                                    batch.cols[batchColIdx].isNull[rowCount]=true;
                                }
                                else
                                {
                                    //((BytesColumnVector) batch.cols[i]).fill(rowData.getBytes(encoding));
                                    //((BytesColumnVector) batch.cols[i]).vector[rowCount]=rowData.getBytes();
                                    ((BytesColumnVector) batch.cols[batchColIdx]).setVal(rowCount,column.asString().getBytes(encoding));
                                }
                                break;
                            /*case BOOLEAN:
                                //recordList.add(column.asBoolean());
                                //((DoubleColumnVector) batch.cols[i]).vector[rowCount] = rowData==null?null:Double.valueOf(rowData).doubleValue();
                                if(rowData==null)
                                {
                                    ((LongColumnVector) batch.cols[i]).fillWithNulls();
                                }
                                else
                                {
                                    ((LongColumnVector) batch.cols[i]).fill((rowData.equalsIgnoreCase("true")?1:0));
                                }
                                break;*/
                            /*case DATE:
                                if(rowData==null)
                                {
                                    //((LongColumnVector) batch.cols[i]).fillWithNulls();
                                    batch.cols[i].noNulls=false;
                                    batch.cols[i].isNull[rowCount]=true;
                                }
                                else
                                {
                                    //((LongColumnVector) batch.cols[i]).fill(Long.valueOf(rowData).longValue());
                                    ((LongColumnVector) batch.cols[i]).vector[rowCount]=Long.valueOf(rowData).longValue();

                                    //((TimestampColumnVector) batch.cols[i]).set(rowCount, new Timestamp(Long.valueOf(rowData)));
                                }
                                break;*/
                            case TIMESTAMP:
                                //recordList.add(new java.sql.Timestamp(column.asDate().getTime()));
                                //((TimestampColumnVector) batch.cols[i]).time[rowCount] = rowData==null?null:(Timestamp.valueOf(rowData)).getTime();
                                if(rowData==null)
                                {
                                    //((TimestampColumnVector) batch.cols[i]).setNullValue(i);
                                    batch.cols[batchColIdx].noNulls=false;
                                    batch.cols[batchColIdx].isNull[rowCount]=true;
                                }
                                else
                                {
                                    //((TimestampColumnVector) batch.cols[i]).fill(Timestamp.valueOf(rowData));
                                    //((TimestampColumnVector) batch.cols[i]).set(rowCount, Timestamp.valueOf(rowData));

                                    ((TimestampColumnVector) batch.cols[batchColIdx]).set(rowCount, new Timestamp(Long.valueOf(rowData)));

                                    //((LongColumnVector) batch.cols[i]).vector[rowCount]=Long.valueOf(rowData).longValue();
                                }
                                break;
                            default:
                                throw DataXException
                                        .asDataXException(
                                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                String.format(
                                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                                        columnConf.getString(Key.NAME),
                                                        columnConf.getString(Key.TYPE)));
                        }
                    } catch (Exception e) {
                        // warn: 此处认为脏数据
                        String message = String.format(
                                "字段类型转换错误：你配置的目标字段 [%s] 为 [%s] 类型，实际字段值为 [%s] .",
                                columnConf.getString(Key.NAME),columnConf.getString(Key.TYPE), rowData);
                        LOG.error("写入数据失败, 将视为脏数据: ",e);
                        taskPluginCollector.collectDirtyRecord(record, message);
                        batch.size--;
                        break;
                    }

                    //字段如果是用于做分区写入的则不会加入到batch的schema里，batch里col的索引也会不一样
                    batchColIdx++;

                    //}else {
                    // warn: it's all ok if nullFormat is null
                    //}
                }
            }
        }
    }

    /**
     * 新版写orcfile类型文件
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    public void orcFileStartWriteNew(RecordReceiver lineReceiver, Configuration config, String fileName,
                                  TaskPluginCollector taskPluginCollector){
        List<Configuration>  columns = config.getListConfiguration(Key.COLUMN);
        String encoding=config.getString(Key.ENCODING,"UTF-8");
        TypeDescription schema=createOrcSchema(null,columns);
        VectorizedRowBatch batch = schema.createRowBatch();

        org.apache.orc.Writer writer=createOrcWriter(fileName,hadoopConf,schema,config);
        Record record = null;

        /*long startNextTime=System.currentTimeMillis();
        long nextTimeSum=0;
        long startTransTime=0;
        long transTimeSum=0;
        long startFlushTime=0;
        long flushTimeSum=0;

        int cnt=1;
        int cntFlush=1;*/

        try {
            while ((record = lineReceiver.getFromReader()) != null) {

                /*nextTimeSum+=(System.currentTimeMillis()-startNextTime);
                if(cnt>=10000)
                {
                    LOG.info("getFromReader平均耗时: "+(1.0*nextTimeSum/10000.0));
                    nextTimeSum=0;
                }*/

                int rowCount = batch.size++;

                //startTransTime=System.currentTimeMillis();

                transportOrcBatch(batch, record,columns, rowCount, taskPluginCollector,encoding,null);

                /*transTimeSum+=System.currentTimeMillis()-startTransTime;
                if(cnt>=10000)
                {
                    LOG.info("trans平均耗时: "+(1.0*transTimeSum/10000.0));
                    transTimeSum=0;
                    cnt=0;
                }*/

                //batch full
                if (batch.size == batch.getMaxSize()) {
                    //startFlushTime=System.currentTimeMillis();
                    writer.addRowBatch(batch);
                    batch.reset();

                    /*flushTimeSum+=System.currentTimeMillis()-startFlushTime;
                    if(cntFlush>=10000)
                    {
                        LOG.info("flush平均耗时: "+(1.0*flushTimeSum/10000.0));
                        flushTimeSum=0;
                        cntFlush=0;
                    }
                    cntFlush++;*/
                }

                //startNextTime=System.currentTimeMillis();
                //cnt++;
            }

            if(batch.size>0){
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        catch(Exception e)
        {
            String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
            LOG.error(message,e);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }

        try
        {
            writer.close();
        }
        catch(Exception e)
        {
            LOG.warn("关闭文件失败: "+fileName,e);
        }
    }

    /*
    通过record得到子目录
     */
    private String getSubDir(List<Integer> subDirsList,Record record,List<Configuration>  columns)
    {
        StringBuilder sb=new StringBuilder();
        for(Integer idx : subDirsList)
        {
            sb.append(columns.get(idx).getString(Key.NAME).toLowerCase()+"="+record.getColumn(idx).getRawData().toString()+ IOUtils.DIR_SEPARATOR);
        }
        return sb.toString();
    }

    /*
    创建相应的文件
     */
    private String buildFileName(String rootPath,String subDir,String fileName)
    {
        String fullPath=rootPath+IOUtils.DIR_SEPARATOR+subDir;

        //如果目录不存在，则创建
        if(!this.isPathexists(fullPath))
        {
            try
            {
                this.fileSystem.mkdirs(new Path(fullPath));
            }
            catch(Exception e)
            {
                String message = String.format("创建目录[%s]时发生IO异常,请检查您的网络是否正常！", fullPath);
                LOG.error(message,e);
                throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
            }
        }

        String tmpSuffix = UUID.randomUUID().toString().replace('-', '_');

        String fullFileName=String.format("%s%s__%s", fullPath, fileName, tmpSuffix);

        /*if (!isEndWithSeparator) {
            tmpFilePath = String.format("%s__%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
        }else if("/".equals(userPath)){
            tmpFilePath = String.format("%s__%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
        }else{
            tmpFilePath = String.format("%s__%s%s", userPath.substring(0,userPath.length()-1), tmpSuffix, IOUtils.DIR_SEPARATOR);
        }*/
        while(this.isPathexists(fullFileName)){

            tmpSuffix = UUID.randomUUID().toString().replace('-', '_');

            fullFileName=String.format("%s%s__%s", fullPath, fileName, tmpSuffix);
        }

        LOG.info("新创建文件: "+fullFileName);
        return fullFileName;
    }

    /**
     * 分区方式写orcfile类型文件
     * @param lineReceiver
     * @param config
     * @param rootPath
     * @param taskPluginCollector
     */
    public void orcFileStartWriteByPartition(RecordReceiver lineReceiver, Configuration config, String rootPath,List<Integer> subDirsList,
                                     TaskPluginCollector taskPluginCollector){
        List<Configuration>  columns = config.getListConfiguration(Key.COLUMN);
        String encoding=config.getString(Key.ENCODING,"UTF-8");
        String fileName = config.getNecessaryValue(Key.FILE_NAME, HdfsWriterErrorCode.REQUIRED_VALUE);

        Record record = null;
        while ((record = lineReceiver.getFromReader()) != null) {

            //得到子目录,比如/20170907000000/1/2
            String subDir=getSubDir(subDirsList,record,columns);
            BufferWriterBatchForPart writerBatch=this.writerBatchMap.get(subDir);

            if(writerBatch==null)
            {
                writerBatch=new BufferWriterBatchForPart();
                TypeDescription schema=createOrcSchema(subDirsList,columns);
                VectorizedRowBatch batch = schema.createRowBatch();

                writerBatch.setBatch(batch);
                writerBatch.setFileName(buildFileName(rootPath,subDir,fileName));

                org.apache.orc.Writer writer=createOrcWriter(writerBatch.getFileName(),hadoopConf,schema,config);
                writerBatch.setWriter(writer);

                this.writerBatchMap.put(subDir,writerBatch);
            }
            int rowCount = writerBatch.getBatch().size++;

            transportOrcBatch(writerBatch.getBatch(), record,columns, rowCount, taskPluginCollector,encoding,subDirsList);

            //batch full
            if (writerBatch.getBatch().size == writerBatch.getBatch().getMaxSize()) {
                try {
                    writerBatch.getWriter().addRowBatch(writerBatch.getBatch());
                }
                catch(Exception e)
                {
                    String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", writerBatch.getFileName());
                    LOG.error(message,e);
                    Path path = new Path(writerBatch.getFileName());
                    deleteDir(path.getParent());
                    throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
                }
                writerBatch.getBatch().reset();
            }
        }

        //flush掉残留的batch并且关闭writer
        for(BufferWriterBatchForPart writerBatch : this.writerBatchMap.values())
        {
            if(writerBatch.getBatch().size>0){
                try {
                    writerBatch.getWriter().addRowBatch(writerBatch.getBatch());
                }
                catch(Exception e)
                {
                    String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", writerBatch.getFileName());
                    LOG.error(message,e);
                    Path path = new Path(writerBatch.getFileName());
                    deleteDir(path.getParent());
                    throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
                }
                writerBatch.getBatch().reset();
            }

            LOG.info("写入完成, 关闭文件 "+writerBatch.getFileName());

            try
            {
                writerBatch.getWriter().close();
            }
            catch(Exception e)
            {
                LOG.warn("关闭文件失败: "+writerBatch.getFileName(),e);
            }
        }
    }

    public List<String> getColumnNames(List<Configuration> columns){
        List<String> columnNames = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            columnNames.add(eachColumnConf.getString(Key.NAME));
        }
        return columnNames;
    }

    /**
     * 根据writer配置的字段类型，构建inspector
     * @param columns
     * @return
     */
    public List<ObjectInspector>  getColumnTypeInspectors(List<Configuration> columns){
        List<ObjectInspector>  columnTypeInspectors = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            SupportHiveDataType columnType = SupportHiveDataType.valueOf(eachColumnConf.getString(Key.TYPE).toUpperCase());
            ObjectInspector objectInspector = null;
            switch (columnType) {
                case TINYINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case SMALLINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case INT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BIGINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case FLOAT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DOUBLE:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case TIMESTAMP:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DATE:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case STRING:
                case VARCHAR:
                case CHAR:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BOOLEAN:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                default:
                    throw DataXException
                            .asDataXException(
                                    HdfsWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                            eachColumnConf.getString(Key.NAME),
                                            eachColumnConf.getString(Key.TYPE)));
            }

            columnTypeInspectors.add(objectInspector);
        }
        return columnTypeInspectors;
    }

    public OrcSerde getOrcSerde(Configuration config){
        String fieldDelimiter = config.getString(Key.FIELD_DELIMITER);
        String compress = config.getString(Key.COMPRESS);
        String encoding = config.getString(Key.ENCODING);

        OrcSerde orcSerde = new OrcSerde();
        Properties properties = new Properties();
        properties.setProperty("orc.bloom.filter.columns", fieldDelimiter);
        properties.setProperty("orc.compress", compress);
        properties.setProperty("orc.encoding.strategy", encoding);

        orcSerde.initialize(conf, properties);
        return orcSerde;
    }

    public static MutablePair<List<Object>, Boolean> transportOneRecord(
            Record record,List<Configuration> columnsConfiguration,
            TaskPluginCollector taskPluginCollector){

        MutablePair<List<Object>, Boolean> transportResult = new MutablePair<List<Object>, Boolean>();
        transportResult.setRight(false);
        List<Object> recordList = Lists.newArrayList();
        int recordLength = record.getColumnNumber();
        if (0 != recordLength) {
            Column column;
            for (int i = 0; i < recordLength; i++) {

                if(i>=columnsConfiguration.size())
                {
                    LOG.error("源端字段的index超出目标端字段的数组下标,源端字段数量为 [{}], 目标端字段数量为 [{}], 请确保你配置的源端字段和目标端字段对应,源端字段最好不要使用*号.",recordLength,columnsConfiguration.size());
                    throw DataXException
                            .asDataXException(
                                    HdfsWriterErrorCode.ILLEGAL_VALUE,
                                    "源端字段的index超出目标端字段的数组下标,请确保你配置的源端字段和目标端字段对应,源端字段最好不要使用*号.");
                }

                column = record.getColumn(i);
                //todo as method
                if (null != column.getRawData()) {
                    String rowData = column.getRawData().toString();
                    Configuration columnConf=null;
                    try {
                        columnConf=columnsConfiguration.get(i);
                    }
                    catch(IndexOutOfBoundsException e)
                    {
                        LOG.error("源端字段的index超出目标端字段的数组下标,源端字段数量为 [{}], 目标端字段数量为 [{}], 请确保你配置的源端字段和目标端字段对应,源端字段最好不要使用*号.",recordLength,columnsConfiguration.size());
                        throw DataXException
                                .asDataXException(
                                        HdfsWriterErrorCode.ILLEGAL_VALUE,
                                        "源端字段的index超出目标端字段的数组下标,请确保你配置的源端字段和目标端字段对应,源端字段最好不要使用*号.");
                    }
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(
                            columnConf.getString(Key.TYPE).toUpperCase());
                    //根据writer端类型配置做类型转换
                    try {
                        switch (columnType) {
                            case TINYINT:
                                recordList.add(Byte.valueOf(rowData));
                                break;
                            case SMALLINT:
                                recordList.add(Short.valueOf(rowData));
                                break;
                            case INT:
                                recordList.add(Integer.valueOf(rowData));
                                break;
                            case BIGINT:
                                recordList.add(column.asLong());
                                break;
                            case FLOAT:
                                recordList.add(Float.valueOf(rowData));
                                break;
                            case DOUBLE:
                                recordList.add(column.asDouble());
                                break;
                            case STRING:
                            case VARCHAR:
                            case CHAR:
                                recordList.add(column.asString());
                                break;
                            case BOOLEAN:
                                recordList.add(column.asBoolean());
                                break;
                            case DATE:
                                recordList.add(new java.sql.Date(column.asDate().getTime()));
                                break;
                            case TIMESTAMP:
                                recordList.add(new java.sql.Timestamp(column.asDate().getTime()));
                                break;
                            default:
                                throw DataXException
                                        .asDataXException(
                                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                String.format(
                                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                                        columnConf.getString(Key.NAME),
                                                        columnConf.getString(Key.TYPE)));
                        }
                    } catch (Exception e) {
                        // warn: 此处认为脏数据
                        String message = String.format(
                                "字段类型转换错误：你目标字段为[%s]类型，实际字段值为[%s].",
                                columnConf.getString(Key.TYPE), column.getRawData().toString());
                        taskPluginCollector.collectDirtyRecord(record, message);
                        transportResult.setRight(true);
                        break;
                    }
                }else {
                    // warn: it's all ok if nullFormat is null
                    recordList.add(null);
                }
            }
        }
        transportResult.setLeft(recordList);
        return transportResult;
    }
}
