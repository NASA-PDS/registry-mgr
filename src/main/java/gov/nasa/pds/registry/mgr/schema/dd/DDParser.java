package gov.nasa.pds.registry.mgr.schema.dd;

import java.io.File;
import java.io.FileReader;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;


public class DDParser
{
    public static interface Callback
    {
        public void onAttribute(DDAttr attr) throws Exception;
    }
    
    // ----------------------------------------------------------------

    public static class DDAttr
    {
        public String id;
        public String dataType;
        public String description;
    }
    
    // ----------------------------------------------------------------
    
    private int attrCount; 
    private JsonReader rd;
    private Callback cb;

    
    public DDParser() throws Exception
    {
    }
    
        
    public void parse(File file, Callback cb) throws Exception
    {
        if(cb == null) throw new IllegalArgumentException("Callback is null");
        this.cb = cb;
        
        rd = new JsonReader(new FileReader(file));
        
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("dataDictionary".equals(name))
                {
                    parseDataDic();
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        
        rd.endArray();
        
        rd.close();
        this.rd = null;
        this.cb = null;
    }
    
    
    private void parseDataDic() throws Exception
    {
        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("attributeDictionary".equals(name))
            {
                parseAttrDic();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }

    
    private void parseAttrDic() throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("attribute".equals(name))
                {
                    parseAttr();
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


    public void parseAttr() throws Exception
    {
        DDAttr attr = new DDAttr();
        
        attrCount++;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                attr.id = rd.nextString();
            }
            else if("dataType".equals(name))
            {
                attr.dataType = rd.nextString();
            }
            else if("description".equals(name))
            {
                attr.description = rd.nextString();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        if(attr.id == null) throw new Exception("Missing identifier in attribute definition. Index = " + attrCount);
        if(attr.dataType == null) throw new Exception("Missing dataType in attribute definition. ID = " + attr.id);
        
        cb.onAttribute(attr);
    }

}
