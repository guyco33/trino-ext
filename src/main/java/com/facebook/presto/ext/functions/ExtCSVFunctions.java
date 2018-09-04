package com.facebook.presto.ext.functions;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import com.facebook.presto.spi.type.ArrayType;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;

public class ExtCSVFunctions {

    private ExtCSVFunctions() {}

    @ScalarFunction("parse_csv_line")
    @Description("parse csv string to array of string values")
    @SqlType("array(varchar)")
    @SqlNullable
    public static Block parseCSVline(
            @SqlType(StandardTypes.VARCHAR) Slice str)
    {
        Reader in = new StringReader(str.toStringUtf8());
        try {
            CSVParser parser = new CSVParser(in, CSVFormat.DEFAULT);
            List<CSVRecord> list = parser.getRecords();
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, list.get(0).size());
            for (String col: list.get(0))
                VARCHAR.writeSlice(blockBuilder, utf8Slice(col));
            return blockBuilder;
        }
        catch (Exception e) {
            return null;
        }
    }

    @ScalarFunction("parse_csv")
    @Description("parse csv string to array of string values")
    @SqlType("array(array(varchar))")
    @SqlNullable
    public static Block parseCSV(
            @SqlType(StandardTypes.VARCHAR) Slice str)
    {
        Reader in = new StringReader(str.toStringUtf8());
        try {
            CSVParser parser = new CSVParser(in, CSVFormat.DEFAULT);
            List<CSVRecord> records = parser.getRecords();
            ArrayType arrayType = new ArrayType(VARCHAR);
            BlockBuilder recordsBuilder = arrayType.createBlockBuilder(null, records.size());
            for (CSVRecord values: records) {
                BlockBuilder valuesBuilder = VARCHAR.createBlockBuilder(null, values.size());
                for (String value : values)
                    VARCHAR.writeSlice(valuesBuilder, utf8Slice(value));
                arrayType.writeObject(recordsBuilder, valuesBuilder.build());
            }
            return recordsBuilder.build();
        }
        catch (Exception e) {
            return null;
        }
    }

}
