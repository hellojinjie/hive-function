package shaman.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import shaman.hive.udf.service.GeoService;

@Description(name="shaman_geo_countrycode", value="_FUNC_ get geo from ip. ")
public class UDFGeoIPCountryCode extends GenericUDF {
    
    private JavaStringObjectInspector stringInspector;
    private GeoService geoService;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("_FUNC_ take only one parameter ip address");
        }
        geoService = GeoService.getInstance();
        stringInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        return stringInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String ip = stringInspector.getPrimitiveJavaObject(arguments[0].get());
        return geoService.getCountryCode(ip);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "get country code from ip address";
    }

}
