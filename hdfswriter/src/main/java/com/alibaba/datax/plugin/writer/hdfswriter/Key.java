package com.alibaba.datax.plugin.writer.hdfswriter;

/**
 * Created by shf on 15/10/8.
 */
public class Key {
    // must have
    public static final String PATH = "path";
    //for partition writer
    public static final String IS_PARTITION_WRITER="isPartitionWriter";
    public static final String ROOT_PATH="rootPath";
    public static final String ROOT_TMP_PATH="rootTmpPath";
    public static final String SUBDIRS="colIndexes";
    //must have
    public final static String DEFAULT_FS = "defaultFS";
    // not must, default NULL
    public final static String HDFS_HA_CONF="hdfsHAConf";
    //must have
    public final static String FILE_TYPE = "fileType";
    // must have
    public static final String FILE_NAME = "fileName";
    // must have for column
    public static final String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String DATE_FORMAT = "dateFormat";
    // must have
    public static final String WRITE_MODE = "writeMode";
    // must have
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    // not must, default UTF-8
    public static final String ENCODING = "encoding";
    // not must, default no compress
    public static final String COMPRESS = "compress";
    //for orc
    public static final String ORC_STRIPE_SIZE="orcStripeSize";
    public static final String ORC_BUFFER_SIZE="orcBufferSize";
    public static final String ORC_BLOCK_SIZE="orcBlockSize";
    public static final String ORC_VERSION="orcVersion";
    // not must, not default \N
    public static final String NULL_FORMAT = "nullFormat";
    // Kerberos
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";


}
