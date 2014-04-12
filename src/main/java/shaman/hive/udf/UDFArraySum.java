package shaman.hive.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(value="_FUNC_ sum values of a array", 
        name="shaman_sum_array")
public class UDFArraySum extends GenericUDF {

    private ListObjectInspector inputOI;
    private PrimitiveObjectInspector valueOI;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("_FUNC_ take one arguments, array");
        }
        this.inputOI = (ListObjectInspector) arguments[0];
        valueOI = (PrimitiveObjectInspector) inputOI.getListElementObjectInspector();
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        
        long result = 0l;
        List<?> list = inputOI.getList(arguments[0].get());
        for (Object l : list) {
            Object primitiveObject = valueOI.getPrimitiveJavaObject(l);
            if (primitiveObject instanceof Integer) {
                result += (Integer) primitiveObject;
            } else if (primitiveObject instanceof Long) {
                result += (Long) primitiveObject;
            } else if (primitiveObject instanceof Short) {
                result += (Short) primitiveObject;
            } else {
                throw new HiveException("can not cast " + primitiveObject);
            }
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Sum the value array elements";
    }

}
