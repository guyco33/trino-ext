package io.trino.ext.functions;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestAuxFunctions
{
    private QueryAssertions assertions;

    public TestAuxFunctions()
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
    public void testAuxFunctions()
    {
        assertThat(assertions.query(
                "SELECT from_utf8('string value')"))
                .matches("VALUES (varchar'string value')");
    }
}
