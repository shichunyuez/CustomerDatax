package com.alibaba.datax.plugin.writer.ftpwriter;

public class Key {
    public static final String PROTOCOL = "protocol";
    
    public static final String HOST = "host";
    
    public static final String USERNAME = "username";
    
    public static final String PASSWORD = "password";
    
    public static final String PORT = "port";   
    
    public static final String TIMEOUT = "timeout";
    
    public static final String CONNECTPATTERN = "connectPattern";
    
    public static final String PATH = "path";

    // not must, default UTF-8
    public static final String ENCODING = "encoding";

    // not must, default no compress
    public static final String COMPRESS = "compress";

    // csv or plain text
    public static final String FILE_FORMAT = "fileFormat";

    // not must , not default ,
    public static final String FIELD_DELIMITER = "fieldDelimiter";

    // writer headers
    public static final String HEADER = "header";

    // not must, not default \N
    public static final String NULL_FORMAT = "nullFormat";

    // not must, date format old style, do not use this
    public static final String FORMAT = "format";
    // for writers ' data format
    public static final String DATE_FORMAT = "dateFormat";

}
