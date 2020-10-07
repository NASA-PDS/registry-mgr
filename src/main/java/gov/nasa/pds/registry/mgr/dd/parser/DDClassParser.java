package gov.nasa.pds.registry.mgr.dd.parser;

import java.util.List;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.mgr.dd.parser.DDParser.Callback;


class DDClassParser
{
    private JsonReader rd;
    private Callback cb;

    
    public DDClassParser(JsonReader rd, Callback cb)
    {
        this.rd = rd;
        this.cb = cb;
    }

    
    public void parse() throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("class".equals(name))
                {
                    parseClass();
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        
        rd.endArray();
    }
    
        
    private void parseClass() throws Exception
    {
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                String id = rd.nextString();
            }
            else if("associationList".equals(name))
            {
                //parseAssocList(ddClass);
                rd.skipValue();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }


    private void parseAssocList(DDClass ddClass) throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("association".equals(name))
                {
                    //List<String> attrIds = parseAssoc();
                    //addAttributes(ddClass, attrIds);
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        
        rd.endArray();
    }

}
