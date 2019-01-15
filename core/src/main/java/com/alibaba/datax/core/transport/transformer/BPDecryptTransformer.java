package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;

import java.util.Arrays;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class BPDecryptTransformer extends Transformer {
    public BPDecryptTransformer() {
        setTransformerName("dx_bpdec");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        int cryptColIdx;
        int decryptColIdx;

        try {
            if (paras.length != 2) {
                throw new RuntimeException("dx_bpdec paras must be 2, arg1 is crypt column index, arg2 is decrypt column index.");
            }

            cryptColIdx = (Integer) paras[0];
            decryptColIdx = Integer.valueOf((String) paras[1]);

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(cryptColIdx);

        try {
            String oriValue = column.asString();
            //如果字段为空，跳过处理
            if(oriValue == null || oriValue.trim().equals("")){
                return record;
            }
            String newValue=GroovyTransformerStaticUtil.BPDec(oriValue);

            record.setColumn(decryptColIdx, new StringColumn(newValue));

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return record;
    }
}
