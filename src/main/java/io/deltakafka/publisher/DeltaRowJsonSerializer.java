package io.deltakafka.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Delta Kernel rows into JSON-friendly Java objects and serializes them with Jackson.
 *
 * <p>Nested structs and arrays are expanded recursively. Binary values are base64-encoded, and
 * Delta dates and timestamps are emitted as ISO-8601 strings.
 */
public final class DeltaRowJsonSerializer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private DeltaRowJsonSerializer() {
    }

    /**
     * Serializes a Delta row into a JSON object string.
     *
     * @param row Delta row to serialize
     * @return JSON representation of the supplied row
     * @throws Exception if Jackson serialization fails
     */
    public static String toJson(Row row) throws Exception {
        return OBJECT_MAPPER.writeValueAsString(toMap(row.getSchema(), row));
    }

    private static Map<String, Object> toMap(StructType schema, Row row) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int index = 0; index < schema.length(); index++) {
            String fieldName = schema.at(index).getName();
            DataType fieldType = schema.at(index).getDataType();
            Object value = row.isNullAt(index) ? null : extractRowValue(row, index, fieldType);
            result.put(fieldName, value);
        }
        return result;
    }

    private static Object extractRowValue(Row row, int index, DataType type) {
        if (type instanceof StringType) {
            return row.getString(index);
        }
        if (type instanceof IntegerType) {
            return row.getInt(index);
        }
        if (type instanceof LongType) {
            return row.getLong(index);
        }
        if (type instanceof BooleanType) {
            return row.getBoolean(index);
        }
        if (type instanceof DoubleType) {
            return row.getDouble(index);
        }
        if (type instanceof FloatType) {
            return row.getFloat(index);
        }
        if (type instanceof ShortType) {
            return row.getShort(index);
        }
        if (type instanceof ByteType) {
            return row.getByte(index);
        }
        if (type instanceof DecimalType) {
            return row.getDecimal(index);
        }
        if (type instanceof BinaryType) {
            return Base64.getEncoder().encodeToString(row.getBinary(index));
        }
        if (type instanceof DateType) {
            return LocalDate.ofEpochDay(row.getInt(index)).toString();
        }
        if (type instanceof TimestampType) {
            long micros = row.getLong(index);
            long seconds = micros / 1_000_000L;
            long nanos = (micros % 1_000_000L) * 1_000L;
            return Instant.ofEpochSecond(seconds, nanos).toString();
        }
        if (type instanceof StructType) {
            return toMap((StructType) type, row.getStruct(index));
        }
        if (type instanceof ArrayType) {
            return extractArray(row.getArray(index), ((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            return extractMap(row.getMap(index), (MapType) type);
        }

        throw new UnsupportedOperationException("Unsupported Delta type: " + type);
    }

    private static List<Object> extractArray(ArrayValue array, DataType elementType) {
        ColumnVector elements = array.getElements();
        List<Object> values = new ArrayList<>(array.getSize());

        for (int index = 0; index < array.getSize(); index++) {
            Object value = elements.isNullAt(index) ? null : extractColumnValue(elements, index, elementType);
            values.add(value);
        }

        return values;
    }

    private static Map<Object, Object> extractMap(MapValue mapValue, MapType mapType) {
        ColumnVector keys = mapValue.getKeys();
        ColumnVector values = mapValue.getValues();
        Map<Object, Object> result = new LinkedHashMap<>();

        for (int index = 0; index < mapValue.getSize(); index++) {
            Object key = extractColumnValue(keys, index, mapType.getKeyType());
            Object value = values.isNullAt(index) ? null : extractColumnValue(values, index, mapType.getValueType());
            result.put(key, value);
        }

        return result;
    }

    private static Object extractColumnValue(ColumnVector vector, int index, DataType type) {
        if (type instanceof StringType) {
            return vector.getString(index);
        }
        if (type instanceof IntegerType) {
            return vector.getInt(index);
        }
        if (type instanceof LongType) {
            return vector.getLong(index);
        }
        if (type instanceof BooleanType) {
            return vector.getBoolean(index);
        }
        if (type instanceof DoubleType) {
            return vector.getDouble(index);
        }
        if (type instanceof FloatType) {
            return vector.getFloat(index);
        }
        if (type instanceof ShortType) {
            return vector.getShort(index);
        }
        if (type instanceof ByteType) {
            return vector.getByte(index);
        }
        if (type instanceof DecimalType) {
            return vector.getDecimal(index);
        }
        if (type instanceof BinaryType) {
            return Base64.getEncoder().encodeToString(vector.getBinary(index));
        }
        if (type instanceof DateType) {
            return LocalDate.ofEpochDay(vector.getInt(index)).toString();
        }
        if (type instanceof TimestampType) {
            long micros = vector.getLong(index);
            long seconds = micros / 1_000_000L;
            long nanos = (micros % 1_000_000L) * 1_000L;
            return Instant.ofEpochSecond(seconds, nanos).toString();
        }

        throw new UnsupportedOperationException("Unsupported nested Delta type: " + type);
    }
}
