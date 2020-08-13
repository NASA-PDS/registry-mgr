package gov.nasa.pds.registry.mgr.util;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

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

    
    private JsonWriter createJsonWriter(Writer writer)
    {
        JsonWriter jw = new JsonWriter(writer);
        if(pretty)
        {
            jw.setIndent("  ");
        }
        
        return jw;
    }
    

    public String createFilterQuery(String field, String value) throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();
        EsQueryUtils.appendFilterQuery(writer, field, value);
        writer.endObject();
        
        writer.close();
        return out.toString();
    }

    
    public String createMatchAllQuery() throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();
        
        writer.name("query");
        writer.beginObject();
        EsQueryUtils.appendMatchAll(writer);
        writer.endObject();

        writer.endObject();
        
        writer.close();
        return out.toString();
    }
    
    
    public String createUpdateStatusJson(String status, String field, String value) throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);
        
        writer.beginObject();
        // Script
        writer.name("script").value("ctx._source.archive_status = '" + status + "'");
        // Query
        EsQueryUtils.appendFilterQuery(writer, field, value);
        writer.endObject();
        
        writer.close();
        return out.toString();
    }

}
