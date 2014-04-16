package shaman.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(value="_FUNC_ get streamType from streamURL. ", 
        name="shaman_streamtype")
public class UDFStreamType extends GenericUDF {

    private JavaStringObjectInspector stringInspector;
    
    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        String streamURL = stringInspector.getPrimitiveJavaObject(arg0[0]);
        
        if (streamURL.contains(".mp4") || streamURL.contains("vod"))
        {
            if (streamURL.contains("live"))
            {
                return 0;
            }
            else
            {
                return 1;
            }
        }
        else
        {
            return 0;
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "Determine Stream Type";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        if (arg0.length != 1) {
            throw new UDFArgumentLengthException("__FUNC__ take one arguments, streamURL");
        }
        this.stringInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }
    
}