package io.trino.ext.functions;

import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.*;
import org.apache.commons.math3.distribution.NormalDistribution;

public class StatsFunctions {

    private StatsFunctions() {}

    @ScalarFunction("normal_dist_invert")
    @Description("return the inverted normal distribution")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double normalDistInvert(
            @SqlType(StandardTypes.DOUBLE ) double value)
    {
        NormalDistribution distribution = new NormalDistribution(0,1);
        return distribution.cumulativeProbability(value);

    }

}
