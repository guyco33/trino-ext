package io.prestosql.ext.functions;

import io.prestosql.spi.Plugin;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class FunctionsPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(AuxFunctions.class)
                .add(CSVFunctions.class)
                .add(JsonFunctions.class)
                .add(StatsFunctions.class)
                .build();
    }
}
