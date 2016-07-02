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

public class ShortFieldType extends NumberFieldType {
	
	public static final String CONTENT_TYPE = "short";

    public ShortFieldType() {
        super(NumericType.INT);
    }

    protected ShortFieldType(ShortFieldType ref) {
        super(ref);
    }

    @Override
    public NumberFieldType clone() {
        return new ShortFieldType(this);
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    @Override
    public Short nullValue() {
        return (Short)super.nullValue();
    }

    @Override
    public Short value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        if (value instanceof BytesRef) {
            return Numbers.bytesToShort((BytesRef) value);
        }
        return Short.parseShort(value.toString());
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        BytesRefBuilder bytesRef = new BytesRefBuilder();
        NumericUtils.intToPrefixCoded(parseValue(value), 0, bytesRef);  // 0 because of exact match
        return bytesRef.get();
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newIntRange(names().indexName(), numericPrecisionStep(),
            lowerTerm == null ? null : (int)parseValue(lowerTerm),
            upperTerm == null ? null : (int)parseValue(upperTerm),
            includeLower, includeUpper);
    }

    @Override
    public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        short iValue = parseValue(value);
        short iSim = fuzziness.asShort();
        return NumericRangeQuery.newIntRange(names().indexName(), numericPrecisionStep(),
            iValue - iSim,
            iValue + iSim,
            true, true);
    }

    @Override
    public FieldStats stats(Terms terms, int maxDoc) throws IOException {
        long minValue = NumericUtils.getMinInt(terms);
        long maxValue = NumericUtils.getMaxInt(terms);
        return new FieldStats.Long(
            maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), minValue, maxValue
        );
    }
    
    private static short parseValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        if (value instanceof BytesRef) {
            return Short.parseShort(((BytesRef) value).utf8ToString());
        }
        return Short.parseShort(value.toString());
    }
}
