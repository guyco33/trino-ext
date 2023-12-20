package io.trino.ext.functions;

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.ext.functions.GzipFunctions.decompress;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGzipFunction
{
    private byte[] compressMessage(byte[] data)
    {
        try {
            ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
            GZIPOutputStream gzipOS = new GZIPOutputStream(byteOS);
            gzipOS.write(data);
            gzipOS.close();
            return byteOS.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDecompress()
    {
        String plainStr = "compressed message";
        assertEquals(
                decompress(wrappedBuffer(compressMessage(plainStr.getBytes(UTF_8)))).toStringUtf8(),
                plainStr);

        assertEquals(
                decompress(utf8Slice("test message")).toStringUtf8(),
                "test message");

        assertNull(decompress(null));

        assertEquals(
                decompress(Slices.EMPTY_SLICE),
                utf8Slice(""));
    }
}
