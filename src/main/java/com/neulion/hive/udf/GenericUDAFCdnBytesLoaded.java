package com.neulion.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(value="", name="bytesloaded")
public class GenericUDAFCdnBytesLoaded implements GenericUDAFResolver2 {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        throw new SemanticException(
              "This UDAF does not support the deprecated getEvaluator() method.");
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        return null;
    }

}
