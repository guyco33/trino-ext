package com.facebook.presto.ext.functions;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;


public class ExtAuxFunctions {

    private ExtAuxFunctions() {}

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
