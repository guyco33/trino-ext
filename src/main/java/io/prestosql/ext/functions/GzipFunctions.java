package io.prestosql.ext.functions;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GzipFunctions {

    private GzipFunctions() {}

    @ScalarFunction("decompress")
    @Description("decompress string")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice decompress(
            @SqlType(StandardTypes.VARBINARY) Slice str)
    {
        if (str == null) {
            return str;
        }
        byte[] data = str.getBytes();
        if (data == null || data.length < 2) {
            return str;
        }
        int magic = data[0] & 0xff | ((data[1] << 8) & 0xff00);
        if (magic == GZIPInputStream.GZIP_MAGIC) {
            try {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
                GZIPInputStream gZIPInputStream = new GZIPInputStream(byteArrayInputStream);
                String res = new String(gZIPInputStream.readAllBytes(), UTF_8);
                gZIPInputStream.close();
                byteArrayInputStream.close();
                return utf8Slice(res);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return str;
    }
}
