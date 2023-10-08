package io.trino.ext.functions;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import io.trino.spi.type.ArrayType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;

public class CSVFunctions {

    private CSVFunctions() {}

    @ScalarFunction("parse_csv")
    @Description("parse csv string to array of string values")
    @SqlType("array(varchar)")
    @SqlNullable
    public static Block parseCSV(
            @SqlType(StandardTypes.VARCHAR) Slice str)
    {
        return parseCSV(str.toStringUtf8(), CSVFormat.DEFAULT.getDelimiter(), '\\');
    }

    @ScalarFunction("parse_csv")
    @Description("parse csv string to array of string values")
    @SqlType("array(varchar)")
    @SqlNullable
    public static Block parseCSV(
            @SqlType(StandardTypes.VARCHAR) Slice str,
            @SqlType("varchar(1)") Slice delimiter
            )
    {
        char cDelimiter =  (delimiter.length() > 0) ?  delimiter.toStringUtf8().charAt(0) : CSVFormat.DEFAULT.getDelimiter();

        return parseCSV(str.toStringUtf8(), cDelimiter, '\\');
    }

    @ScalarFunction("parse_csv")
    @Description("parse csv string to array of string values")
    @SqlType("array(varchar)")
    @SqlNullable
    public static Block parseCSV(
            @SqlType(StandardTypes.VARCHAR) Slice str,
            @SqlType("varchar(1)") Slice delimiter,
            @SqlType(StandardTypes.CHAR) Slice escape
    )
    {
        char cDelimiter =  (delimiter.length() > 0) ?  delimiter.toStringUtf8().charAt(0) : CSVFormat.DEFAULT.getDelimiter();
        char cEscape = (escape.length() > 0) ?  escape.toStringUtf8().charAt(0) : '\\';

        return parseCSV(str.toStringUtf8(), cDelimiter, cEscape);
    }


    @ScalarFunction("parse_csv")
    @Description("parse csv string to array of string values")
    @SqlType("array(array(varchar))")
    @SqlNullable
    public static Block parseCSV(
            @SqlType(StandardTypes.VARCHAR) Slice str,
            @SqlType("varchar(1)") Slice delimiter,
            @SqlType(StandardTypes.CHAR) Slice escape,
            @SqlType(StandardTypes.VARCHAR) Slice record_seperator
            )
    {
        char cDelimiter =  (delimiter.length() > 0) ?  delimiter.toStringUtf8().charAt(0) : CSVFormat.DEFAULT.getDelimiter();
        char cEscape = (escape.length() > 0) ?  escape.toStringUtf8().charAt(0) : '\\';
        String cRecordSperator = record_seperator.toStringUtf8();

       return parseCSV(str.toStringUtf8(),cDelimiter , cEscape , cRecordSperator);
    }


    public static Block parseCSV(String str, char delimiter, char escape) {
        Reader in = new StringReader(str);
        try {
            CSVFormat format = CSVFormat.DEFAULT.withDelimiter(delimiter);
            if (escape != '\\')
                format = format.withEscape(escape);
            CSVParser parser = new CSVParser(in, format);
            List<CSVRecord> list = parser.getRecords();
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, list.get(0).size());
            for (String value: list.get(0))
                VARCHAR.writeSlice(blockBuilder, utf8Slice(value));
            return blockBuilder.build();
        }
        catch (Exception e) {
            return null;
        }
    }

    public static Block parseCSV(String str, char delimiter, char escape, String recordSeparator) {
        Reader in = new StringReader(str);
        try {
            CSVFormat format = CSVFormat.DEFAULT.withDelimiter(delimiter).withRecordSeparator(recordSeparator);
            if (escape != '\\')
                format = format.withEscape(escape);
            CSVParser parser = new CSVParser(in, format);
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
