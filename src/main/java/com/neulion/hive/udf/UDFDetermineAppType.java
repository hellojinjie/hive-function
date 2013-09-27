package com.neulion.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class UDFDetermineAppType extends GenericUDF {

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        return null;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return null;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        return null;
    }
    
}
