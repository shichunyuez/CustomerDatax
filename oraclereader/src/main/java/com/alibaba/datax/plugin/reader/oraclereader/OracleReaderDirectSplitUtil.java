package com.alibaba.datax.plugin.reader.oraclereader;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.*;
import com.alibaba.datax.plugin.rdbms.reader.util.HintUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/6/21 0021.
 */
public class OracleReaderDirectSplitUtil {

    private static final Logger LOG = LoggerFactory
            .getLogger(OracleReaderDirectSplitUtil.class);

    public static List<Configuration> doSplit(Configuration originalSliceConfig, int adviceNumber)
    {
        boolean isTableMode = originalSliceConfig.getBool(com.alibaba.datax.plugin.rdbms.reader.Constant.IS_TABLE_MODE).booleanValue();
        int eachTableShouldSplittedNumber = adviceNumber;
        int tableNumber=originalSliceConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.TABLE_NUMBER_MARK);
        if(tableNumber>1)
        {
            LOG.error("direct模式目前不支持多个表.");
            throw DataXException
                    .asDataXException(DBUtilErrorCode.CONF_ERROR,
                            String.format("您配置的 table 有误，direct模式目前不支持多个表."));
        }
        if (!isTableMode) {
            LOG.error("direct模式目前只支持表模式.");
            throw DataXException
                    .asDataXException(DBUtilErrorCode.CONF_ERROR,
                            String.format("您的配置有误，direct模式目前只支持表模式."));
        }

        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);
        String username = originalSliceConfig.getString(Key.USERNAME);
        String password = originalSliceConfig.getString(Key.PASSWORD);
        String directMode=originalSliceConfig.getString(Key.DIRECT_MODE,"range");
        if(directMode!=null)
        {
            directMode=directMode.toLowerCase();
        }
        int subSQLCnt=originalSliceConfig.getInt(Key.DIRECT_UNIONALL_SUBSQL_CNT,200);

        List<Object> conns = originalSliceConfig.getList(com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK, Object.class);

        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration sliceConfig = originalSliceConfig.clone();

            Configuration connConf = Configuration.from(conns.get(i).toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            // 抽取 jdbcUrl 中的 ip/port 进行资源使用的打标，以提供给 core 做有意义的 shuffle 操作
            sliceConfig.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));

            sliceConfig.remove(com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK);

            Configuration tempSlice;

            // 说明是配置的 table 方式
            if (isTableMode) {
                List<String> tables = connConf.getList(Key.TABLE, String.class);
                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");
                Validate.isTrue(tables.size()==1, "您读取数据库表配置错误,direct模式目前不支持多个表.");

                //实际上只有一个表
                for (String table : tables) {
                    Validate.isTrue(null != table && !table.trim().equals(""), "您读取数据库表配置错误.");
                    LOG.warn("direct模式将忽略hint.");
                    //direct模式将忽略用户自定义的hint
                    String queryColumn = buildSelectColumnsWithDirectHint(table, column);
                    //range模式
                    if(directMode.equals("range"))
                    {
                        LOG.info("direct模式将使用按Range分区方式.");
                        List<JSONObject> rowidRanges= DBUtil.buildRowIDRangeByNumber(jdbcUrl,username,password,getSchema(originalSliceConfig,table),getTableWithoutSchema(table),eachTableShouldSplittedNumber,"range",subSQLCnt);
                        for(JSONObject rowidRange : rowidRanges)
                        {
                            String originalSQL=SingleTableSplitUtil.buildQuerySql(queryColumn, table, where);
                            if (StringUtils.isBlank(where)) {
                                originalSQL=originalSQL+" WHERE ROWID "+rowidRange.getString("startopt")+" '"+rowidRange.getString("start")+"' AND ROWID "+rowidRange.getString("endopt")+" '"+rowidRange.getString("end")+"'";
                            }
                            else
                            {
                                originalSQL=originalSQL+" AND ROWID "+rowidRange.getString("startopt")+" '"+rowidRange.getString("start")+"' AND ROWID "+rowidRange.getString("endopt")+" '"+rowidRange.getString("end")+"'";
                            }
                            tempSlice = sliceConfig.clone();
                            tempSlice.set(Key.TABLE, table);
                            tempSlice.set(Key.QUERY_SQL, originalSQL);
                            splittedConfigs.add(tempSlice);
                        }
                    }
                    //union all模式 有绑定变量
                    else{
                        LOG.info("direct模式将使用按Union All并绑定变量的分区方式.");
                        List<JSONObject> splitConfs= DBUtil.buildRowIDRangeByNumber(jdbcUrl,username,password,getSchema(originalSliceConfig,table),getTableWithoutSchema(table),eachTableShouldSplittedNumber,"union",subSQLCnt);
                        for(JSONObject splitConf : splitConfs)
                        {
                            String originalSQL=SingleTableSplitUtil.buildQuerySql(queryColumn, table, where);
                            if (StringUtils.isBlank(where)) {
                                originalSQL=originalSQL+" WHERE ROWID >= ? AND ROWID < ?";
                            }
                            else
                            {
                                originalSQL=originalSQL+" AND ROWID >= ? AND ROWID < ?";
                            }
                            String finalSQL=new String(originalSQL);
                            //注意,即使实际的绑定变量不足数量,也会拼接出固定数量的占位符,在实际处理的时候,如果绑定变量不足,则绑定空字符串,不影响最后的结果集
                            for(int unionCnt=1; unionCnt<subSQLCnt; unionCnt++)
                            {
                                finalSQL=finalSQL+" UNION ALL "+originalSQL;
                            }
                            JSONArray binds=splitConf.getJSONArray(Key.BINDS);
                            //LOG.info("binds的值为: "+binds);
                            tempSlice = sliceConfig.clone();
                            tempSlice.set(Key.TABLE, table);
                            tempSlice.set(Key.QUERY_SQL, finalSQL);
                            tempSlice.set(Key.QUERY_SQL_BIND_VARS, binds);
                            tempSlice.set(Key.BIND_VAL_CNT,subSQLCnt*2);
                            splittedConfigs.add(tempSlice);
                        }
                    }
                }
            } else {
                // 说明是配置的 querySql 方式
                /*List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);
                Validate.isTrue(null != sqls && !sqls.isEmpty(), "您读取QuerySQL配置错误.");
                Validate.isTrue(sqls.size()>1, "您读取QuerySQL配置错误,direct模式目前不支持多条QuerySQL.");
                for (String querySql : sqls) {
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.QUERY_SQL, querySql);
                    splittedConfigs.add(tempSlice);
                }*/

                LOG.error("direct方式目前不支持QuerySQL.");
                throw DataXException
                        .asDataXException(DBUtilErrorCode.CONF_ERROR,
                                String.format("您的配置有误，direct模式目前不支持QuerySQL."));
            }

        }

        return splittedConfigs;
    }

    private static String getTableWithoutSchema(String table)
    {
        String[] tableStr = table.split("\\.");
        return tableStr[tableStr.length-1];
    }

    private static String getSchema(Configuration originalSliceConfig,String table)
    {
        String[] tableStr = table.split("\\.");
        if(tableStr.length>=2)
        {
            return tableStr[0];
        }
        else
        {
            return originalSliceConfig.getString(Key.USERNAME);
        }
    }

    //添加/*+ROWID(TABLE)*/ 提示
    private static String buildSelectColumnsWithDirectHint(String table, String column){

        try{
            String[] tableStr = table.split("\\.");
            String tableWithoutSchema = tableStr[tableStr.length-1];
            return "/*+ROWID("+tableWithoutSchema+")*/ "+column;
        } catch (Exception e){
            LOG.warn("添加ROWID提示错误,将不使用ROWID提示,可能会导致严重的性能问题.", e);
        }
        return column;
    }

    private static int calculateEachTableShouldSplittedNumber(int adviceNumber,
                                                              int tableNumber) {
        double tempNum = 1.0 * adviceNumber / tableNumber;

        return (int) Math.ceil(tempNum);
    }
}
