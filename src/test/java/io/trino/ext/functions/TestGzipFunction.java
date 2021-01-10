package io.trino.ext.functions;

import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.*;

public class TestGzipFunction {

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
    public void testDecompress() {
        assertEquals(
                GzipFunctions.decompress(wrappedBuffer(compressMessage("compressed message".getBytes(UTF_8)))).toStringUtf8(),
                "compressed message");

        assertEquals(
                GzipFunctions.decompress(utf8Slice("test message")).toStringUtf8(),
                "test message");

        assertNull(GzipFunctions.decompress(null));

        assertEquals(
                GzipFunctions.decompress(Slices.EMPTY_SLICE),
                utf8Slice(""));
    }
}
