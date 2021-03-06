package shaman.hive.udf;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(value="_FUNC_ sum values of a map", 
        name="shaman_sum_mapvalue")
public class UDFMapValueSum extends GenericUDF {

    private MapObjectInspector inputOI;
    private PrimitiveObjectInspector valueOI;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("_FUNC_ take one arguments, map");
        }
        this.inputOI = (MapObjectInspector) arguments[0];
        valueOI = (PrimitiveObjectInspector) inputOI.getMapValueObjectInspector();
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        
        long result = 0l;
        Map<?, ?> map = inputOI.getMap(arguments[0].get());
        for (Object l : map.values()) {
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
        return "Sum the value of map value";
    }

}
