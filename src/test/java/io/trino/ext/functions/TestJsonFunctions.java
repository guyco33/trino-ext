package io.trino.ext.functions;

import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.testng.Assert.*;

public class TestJsonFunctions {

    @Test
    public void testGetJsonKeys() throws IOException {
        assertEquals(
                JsonFunctions.getJsonKeys("{\"x\":123, \"y\":12}", true),
                Arrays.asList("x","y"));

        System.out.println(System.getProperty("user.dir"));
        String str = new String(Files.readAllBytes(Paths.get("resources/test.json")));
        assertEquals(
                JsonFunctions.getJsonKeys(str,true),
                Arrays.asList(
                        "created_at",
                        "data:attr1:attr11",
                        "data:attr1:attr12",
                        "data:attr1:attr13",
                        "data:attr2",
                        "data:attr3:attr31:attr311:attr3111*:attr3111a",
                        "data:attr3:attr31:attr311:attr3111*:attr3111b",
                        "data:attr3:attr31:attr311:attr3111*:attr3111c*:attr3111c1:attr3111c11",
                        "data:attr3:attr31:attr311:attr3111*:attr3111c*:attr3111c1:attr3111c12",
                        "data:attr3:attr31:attr311:attr3111*:attr3111c*:attr3111c2",
                        "data:attr4",
                        "event_name",
                        "id"
                ));
    }

}