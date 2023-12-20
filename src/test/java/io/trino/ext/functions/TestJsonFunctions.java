package io.trino.ext.functions;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static io.trino.ext.functions.JsonFunctions.getJsonKeys;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonFunctions
{
    private QueryAssertions assertions;

    public TestJsonFunctions()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new FunctionsPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testGetJsonKeys() throws IOException
    {
        assertEquals(
                getJsonKeys("{\"x\":123, \"y\":12}", true),
                Arrays.asList("/x","/y"));

        assertThat(assertions.query(
                "SELECT get_json_keys('{\"x\":123, \"y\":12}')"))
                .matches("VALUES (Array[varchar'/x', varchar'/y'])");

        System.out.println(System.getProperty("user.dir"));
        String str = new String(Files.readAllBytes(Paths.get("resources/test.json")));
        assertEquals(
                getJsonKeys(str,true),
                Arrays.asList(
                        "/created_at",
                        "/data/attr1/attr11",
                        "/data/attr1/attr12",
                        "/data/attr1/attr13",
                        "/data/attr2",
                        "/data/attr3/attr31/attr311/attr3111*/attr3111a",
                        "/data/attr3/attr31/attr311/attr3111*/attr3111b",
                        "/data/attr3/attr31/attr311/attr3111*/attr3111c*/attr3111c1/attr3111c11",
                        "/data/attr3/attr31/attr311/attr3111*/attr3111c*/attr3111c1/attr3111c12",
                        "/data/attr3/attr31/attr311/attr3111*/attr3111c*/attr3111c2",
                        "/data/attr4",
                        "/event_name",
                        "/id"
                ));
    }
}
