package com.neulion.hive.udf;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.google.common.collect.Maps;

@Description(value="_FUNC_ calculate the cdn bytes loaded", name="cdnbytesloaded")
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
        private Map<String, String> cdnPatterns;
        
        {
            cdnPatterns = Maps.newHashMap();
            cdnPatterns.put("cdnl3nl", "Level3");
            cdnPatterns.put("cdnak", "Akamai");
            cdnPatterns.put("cndllnwnl", "LimeLight");
            cdnPatterns.put("cdncd", "CDNetworks");
        }
        
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (parameters.length != 1) {
                throw new UDFArgumentLengthException("In GenericUDAFCdnBytesLoadedEvaluator "
                        + "parameters length is not 1");
            }
            inputOI = (MapObjectInspector) parameters[0];
            outputOI = (MapObjectInspector) ObjectInspectorUtils
                    .getStandardObjectInspector(inputOI, ObjectInspectorCopyOption.JAVA);
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
            merge(agg, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return ((CdnAggregationBuffer) agg).bytesLoaded;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            CdnAggregationBuffer buffer = (CdnAggregationBuffer) agg;
            @SuppressWarnings("unchecked")
            Map<String, Long> bytesLoaded = (Map<String, Long>) inputOI.getMap(partial);
            for (String key : bytesLoaded.keySet()) {
                boolean matched = false;
                for (String pattern : cdnPatterns.keySet()) {
                    if (key.contains(pattern)) {
                        String cdnName = cdnPatterns.get(pattern);
                        Long loaded = buffer.bytesLoaded.get(cdnName);
                        if (loaded == null) {
                            loaded = bytesLoaded.get(key);
                        } else {
                            loaded = loaded > bytesLoaded.get(key) ? loaded : bytesLoaded.get(key);
                        }
                        buffer.bytesLoaded.put(cdnName, loaded);
                        matched = true;
                        break;
                    }
                }
                if (matched == false) {
                    Long loaded = buffer.bytesLoaded.get(key);
                    if (loaded == null) {
                        loaded = bytesLoaded.get(key);
                    } else {
                        loaded = loaded > bytesLoaded.get(key) ? loaded : bytesLoaded.get(key);
                    }
                    buffer.bytesLoaded.put(key, loaded);
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            return ((CdnAggregationBuffer) agg).bytesLoaded;
        }
        
    }
}
