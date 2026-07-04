package com.openexchange.oms.api.dto;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.match.domain.FixedPoint;

import java.io.IOException;

/**
 * Jackson codec for money on the wire (oms#39 / P4.1).
 *
 * Internally money is 8-dp fixed-point (long). On the wire it is the
 * canonical decimal STRING produced by {@link FixedPoint#format}, because the
 * old double path was lossy twice over: JSON number → IEEE double →
 * fromDouble rounding. {@code parse(format(x)) == x} for every long, so a
 * value survives any number of API round-trips bit-exactly.
 */
public final class FixedPointJson {

    private FixedPointJson() {
    }

    /** Serializes an internal fixed-point long as an exact 8-dp decimal string. */
    public static final class Serializer extends JsonSerializer<Long> {
        @Override
        public void serialize(Long value, com.fasterxml.jackson.core.JsonGenerator gen, SerializerProvider sp)
                throws IOException {
            gen.writeString(FixedPoint.format(value));
        }
    }

    /**
     * Accepts the canonical decimal string (exact), or a legacy JSON number
     * (DEPRECATED: rounded through the double path; kept one release so
     * pre-freeze clients and harnesses keep working). Null/absent = 0.
     */
    public static final class Deserializer extends JsonDeserializer<Long> {
        @Override
        public Long deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
            JsonToken t = p.currentToken();
            if (t == JsonToken.VALUE_STRING) {
                String text = p.getText();
                try {
                    return FixedPoint.parse(text);
                } catch (NumberFormatException e) {
                    throw new IOException("invalid money string '" + text
                            + "' (expected decimal with at most 8 fractional digits)", e);
                } catch (FixedPoint.OverflowException e) {
                    throw new IOException("money value '" + text + "' out of range", e);
                }
            }
            if (t == JsonToken.VALUE_NUMBER_INT || t == JsonToken.VALUE_NUMBER_FLOAT) {
                double d = p.getDoubleValue();
                if (!Double.isFinite(d)) {
                    throw new IOException("money value must be finite");
                }
                return FixedPoint.fromDouble(d);
            }
            if (t == JsonToken.VALUE_NULL) {
                return 0L;
            }
            throw new IOException("money value must be a decimal string (or a deprecated JSON number)");
        }

        @Override
        public Long getNullValue(DeserializationContext ctx) {
            return 0L;
        }
    }
}
