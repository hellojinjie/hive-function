package shaman.hive.udf;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Maps;

@Description(value="_FUNC_ calculate the cdn bytes loaded", name="shaman_cdnbytesloaded")
public class GenericUDAFCdnBytesLoaded extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        throw new SemanticException(
              "This UDAF does not support the deprecated getEvaluator() method.");
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        ObjectInspector[] ois = info.getParameterObjectInspectors();
        if (ois.length != 1) {
            throw new UDFArgumentLengthException("_FUNC_ take only one parameter");
        }
        return new GenericUDAFCdnBytesLoadedEvaluator();
    }

    public static class CdnAggregationBuffer implements AggregationBuffer {
        Map<String, Long> bytesLoaded;
    }
    
    public static class GenericUDAFCdnBytesLoadedEvaluator extends GenericUDAFEvaluator {
        
        private MapObjectInspector inputOI;
        private MapObjectInspector outputOI;
        
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (parameters.length != 1) {
                throw new UDFArgumentLengthException("In GenericUDAFCdnBytesLoadedEvaluator "
                        + "parameters length is not 1");
            }
            inputOI = (MapObjectInspector) parameters[0];
            System.out.println(inputOI.getTypeName());
            outputOI = (MapObjectInspector) ObjectInspectorUtils
                    .getStandardObjectInspector(inputOI, ObjectInspectorCopyOption.JAVA);
            System.out.println(outputOI.getTypeName());
            return outputOI;
        }
        
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CdnAggregationBuffer buffer = new CdnAggregationBuffer();
            buffer.bytesLoaded = Maps.newHashMap();
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            CdnAggregationBuffer buffer = (CdnAggregationBuffer) agg;
            buffer.bytesLoaded.clear();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            if (parameters.length > 1 && parameters[0] != null) {
                try {
                    CdnAggregationBuffer buffer = (CdnAggregationBuffer) agg;
                    @SuppressWarnings("unchecked")
                    Map<String, Long> bytesLoaded = (Map<String, Long>) inputOI.getMap(parameters[0]);
                    for (String key : bytesLoaded.keySet()) {
                        Long bytes = bytesLoaded.get(key);
                        if (bytes < 0) {
                            continue;
                        }
                        Long loaded = buffer.bytesLoaded.get(key);
                        if (loaded == null) {
                            loaded = bytes;
                        } else {
                            loaded = loaded + bytes;
                        }
                        buffer.bytesLoaded.put(key, loaded);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return ((CdnAggregationBuffer) agg).bytesLoaded;
        }

        /**
         * merge is called at reduce phase.
         * But why inputOI.getMap change to return Map<Text, LongWritable>
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            try {
                CdnAggregationBuffer buffer = (CdnAggregationBuffer) agg;
                @SuppressWarnings("unchecked")
                Map<Text, LongWritable> bytesLoaded = (Map<Text, LongWritable>) inputOI.getMap(partial);
                for (Text key : bytesLoaded.keySet()) {
                    Long loaded = buffer.bytesLoaded.get(key.toString());
                    if (loaded == null) {
                        loaded = bytesLoaded.get(key).get();
                    } else {
                        loaded = loaded + bytesLoaded.get(key).get();
                    }
                    buffer.bytesLoaded.put(key.toString(), loaded);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            return ((CdnAggregationBuffer) agg).bytesLoaded;
        }
        
    }
}
