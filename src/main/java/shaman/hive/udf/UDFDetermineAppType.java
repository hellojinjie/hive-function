package shaman.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(value="_FUNC_ get appType from deviceType and appType. "
        + "Some message miss appType field, we need to determine the appType from deviceType", 
        name="shaman_apptype")
public class UDFDetermineAppType extends GenericUDF {

    private JavaStringObjectInspector stringInspector;
    private AppTypeMapping appTypeMapping;
    
    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        String deviceType = stringInspector.getPrimitiveJavaObject(arg0[0].get());
        String appType = stringInspector.getPrimitiveJavaObject(arg0[1].get());
        if ("".equals(appType)) {
            appType = appTypeMapping.get(deviceType);
            if (appType == null) {
                return "";
            } else {
                return appType;
            }
        } else {
            return appType;
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "Determine App Type";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        if (arg0.length != 2) {
            throw new UDFArgumentLengthException("_FUNC_ take two arguments, "
                    + "first is the deviceType, second is the appType");
        }
        this.appTypeMapping = AppTypeMapping.getInstance();
        this.stringInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }
    
}