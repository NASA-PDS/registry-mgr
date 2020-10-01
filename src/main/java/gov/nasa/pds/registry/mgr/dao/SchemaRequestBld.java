package gov.nasa.pds.registry.mgr.dao;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

import com.google.gson.stream.JsonWriter;


public class SchemaRequestBld
{
    private boolean pretty;

    
    public SchemaRequestBld(boolean pretty)
    {
        this.pretty = pretty;
    }

    
    public SchemaRequestBld()
    {
        this(false);
    }

    
    protected JsonWriter createJsonWriter(Writer writer)
    {
        JsonWriter jw = new JsonWriter(writer);
        if (pretty)
        {
            jw.setIndent("  ");
        }

        return jw;
    }

    
    public String createMgetRequest(List<String> ids) throws IOException
    {
        StringWriter wr = new StringWriter();
        JsonWriter jw = createJsonWriter(wr);

        jw.beginObject();
        jw.name("ids");
        
        jw.beginArray();
        for(String id: ids)
        {
            jw.value(id);
        }
        jw.endArray();
        
        jw.endObject();
        jw.close();        

        return wr.toString();        
    }
}
