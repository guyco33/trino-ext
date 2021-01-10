package io.trino.ext.functions;

import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.airlift.slice.Slice;


public class AuxFunctions {

    private AuxFunctions() {}

    @ScalarFunction("from_utf8")
    @Description("dummy function that returns same input string")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice fromUtf8(
            @SqlType(StandardTypes.VARCHAR) Slice str)
    {
        return str;
    }

}
