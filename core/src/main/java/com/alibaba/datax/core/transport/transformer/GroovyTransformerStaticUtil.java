package com.alibaba.datax.core.transport.transformer;

//import bestpay.tools.union.utils.UnionUtilEx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GroovyTransformer的帮助类，供groovy代码使用，必须全是static的方法
 * Created by liqiang on 16/3/4.
 */
public class GroovyTransformerStaticUtil  {

    private static final Logger LOG = LoggerFactory.getLogger(GroovyTransformerStaticUtil.class);

    /*private static UnionUtilEx unionUtil =UnionUtilEx.getInstance();
    static
    {
        try
        {
            unionUtil.initSecSuiteUnion("****", 6666, 3, "HNNX", "HNNX", 3, "****","****");
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
