package gov.nasa.pds.registry.mgr.util;

import java.io.IOException;
import java.io.StringWriter;

import com.google.gson.stream.JsonWriter;

public class EsQueryBuilder
{
    private boolean pretty;

    
    public EsQueryBuilder(boolean pretty)
    {
        this.pretty = pretty;
    }

    
    public EsQueryBuilder()
    {
        this(false);
    }

    
    public String createUpdateStatusJson(String status, String field, String value) throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = new JsonWriter(out);
        if(pretty)
        {
            writer.setIndent("  ");
        }
        
        writer.beginObject();
        // Script
        writer.name("script").value("ctx._source.archive_status = '" + status + "'");
        // Query
        appendFilterQuery(writer, field, value);
        writer.endObject();        
        
        writer.close();
        return out.toString();
    }
    

    private static void appendFilterQuery(JsonWriter writer, String field, String value) throws IOException
    {
        writer.name("query");
        writer.beginObject();

        writer.name("bool");
        writer.beginObject();
        appendMustMatchAll(writer);
        appendTermFilter(writer, field, value);
        writer.endObject();

        writer.endObject();
    }
    
    
    private static void appendMustMatchAll(JsonWriter writer) throws IOException
    {
        writer.name("must");
        writer.beginObject();

        writer.name("match_all");
        writer.beginObject();
        writer.endObject();

        writer.endObject();
    }


    private static void appendTermFilter(JsonWriter writer, String field, String value) throws IOException
    {
        writer.name("filter");
        writer.beginObject();

        writer.name("term");
        writer.beginObject();
        writer.name(field).value(value);
        writer.endObject();

        writer.endObject();
    }

}
