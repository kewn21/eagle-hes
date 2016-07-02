package org.elasticsearch.index.mapper.core;

import java.io.IOException;

import org.apache.lucene.index.Terms;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.core.NumberFieldMapper.NumberFieldType;

public class FloatFieldType extends NumberFieldType {
	
	public static final String CONTENT_TYPE = "float";

    public FloatFieldType() {
        super(NumericType.FLOAT);
    }

    protected FloatFieldType(FloatFieldType ref) {
        super(ref);
    }

    @Override
    public NumberFieldType clone() {
        return new FloatFieldType(this);
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    @Override
    public Float nullValue() {
        return (Float)super.nullValue();
    }

    @Override
    public Float value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof BytesRef) {
            return Numbers.bytesToFloat((BytesRef) value);
        }
        return Float.parseFloat(value.toString());
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        int intValue = NumericUtils.floatToSortableInt(parseValue(value));
        BytesRefBuilder bytesRef = new BytesRefBuilder();
        NumericUtils.intToPrefixCoded(intValue, 0, bytesRef);   // 0 because of exact match
        return bytesRef.get();
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newFloatRange(names().indexName(), numericPrecisionStep(),
            lowerTerm == null ? null : parseValue(lowerTerm),
            upperTerm == null ? null : parseValue(upperTerm),
            includeLower, includeUpper);
    }

    @Override
    public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        float iValue = parseValue(value);
        final float iSim = fuzziness.asFloat();
        return NumericRangeQuery.newFloatRange(names().indexName(), numericPrecisionStep(),
            iValue - iSim,
            iValue + iSim,
            true, true);
    }

    @Override
    public FieldStats stats(Terms terms, int maxDoc) throws IOException {
        float minValue = NumericUtils.sortableIntToFloat(NumericUtils.getMinInt(terms));
        float maxValue = NumericUtils.sortableIntToFloat(NumericUtils.getMaxInt(terms));
        return new FieldStats.Float(
            maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), minValue, maxValue
        );
    }
    
    private static float parseValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof BytesRef) {
            return Float.parseFloat(((BytesRef) value).utf8ToString());
        }
        return Float.parseFloat(value.toString());
    }
}
