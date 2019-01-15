package com.alibaba.datax.server.transport.transformer;

//import bestpay.tools.union.utils.UnionUtilEx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GroovyTransformer的帮助类，供groovy代码使用，必须全是static的方法
 * Created by liqiang on 16/3/4.
 */
public class GroovyTransformerStaticUtil  {

    private static final Logger LOG = LoggerFactory.getLogger(GroovyTransformerStaticUtil.class);

    //private static UnionUtilEx unionUtil =UnionUtilEx.getInstance();
    /*static
    {
        try
        {
            unionUtil.initSecSuiteUnion("172.28.0.246", 6666, 3, "HNNX", "HNNX", 3, "7504E0520C32FB7D41072D46E810B2A709CD668AD6FD22A595DCDAF3A507B81F","APPDATA.0001.ZEK");
            LOG.info("初始化bestpay加解密模块成功...");
        }
        catch(Exception e)
        {
            LOG.error("初始化bestpay加解密模块出错: ",e);
        }
    }*/

    public static String BPDec(String encData)
    {
        //return unionUtil.decryptByAES(encData).getResult();
        return "";
    }
}
