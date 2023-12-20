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
public class TestCSVFunctions
{
    private QueryAssertions assertions;

    public TestCSVFunctions()
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
    public void testCSVFunctions() {
        assertThat(assertions.query(
                "SELECT parse_csv('23,45,45')"))
                .matches("VALUES (array[varchar'23', varchar'45', varchar'45'])");

        assertThat(assertions.query(
                "SELECT parse_csv('23,\"45,45\"')"))
                .matches("VALUES (array[varchar'23', varchar'45,45'])");

        assertThat(assertions.query(
                "SELECT parse_csv('23\t\"45,45\"')"))
                .matches("VALUES (array[varchar'23\t\"45', varchar'45\"'])");

        assertThat(assertions.query(
                "SELECT parse_csv('23\t\"4545\"')"))
                .matches("VALUES (array[varchar'23\t\"4545\"'])");

    }

    @Test
    public void testCSVFunctionsWithSeparator()
    {
        assertThat(assertions.query(
                "SELECT parse_csv('23\t\"45,45\"','\t')"))
                .matches("VALUES (array[varchar'23', varchar'45,45'])");
    }
}
