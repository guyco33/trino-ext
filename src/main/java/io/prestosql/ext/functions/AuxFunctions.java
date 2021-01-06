package io.prestosql.ext.functions;

import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
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
