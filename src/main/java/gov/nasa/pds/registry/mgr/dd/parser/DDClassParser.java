package gov.nasa.pds.registry.mgr.dd.parser;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.mgr.dd.parser.DDParser.Callback;


class DDClassParser
{
    private JsonReader rd;
    private Callback cb;
    private int itemCount;

    
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
        itemCount++;
        
        DDClass ddClass = new DDClass();
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                String id = rd.nextString();
                
                String tokens[] = id.split("\\.");
                if(tokens.length != 3) throw new Exception("Could not parse class id " + id);
                
                ddClass.classNs = tokens[1];
                ddClass.className = tokens[2];
            }
            else if("associationList".equals(name))
            {
                parseAssocList(ddClass);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        if(ddClass.className == null)
        {
            String msg = "Missing identifier in class definition. Index = " + itemCount;
            throw new Exception(msg);
        }

        cb.onClass(ddClass);
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
                    parseAssoc(ddClass);
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

    
    private void parseAssoc(DDClass ddClass) throws Exception
    {
        String id = null;
        boolean isParentOf = false;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("assocType".equals(name))
            {
                String val = rd.nextString();
                if("parent_of".equals(val))
                {
                    isParentOf = true;
                }
            }
            else if("attributeId".equals(name) && isParentOf)
            {
                id = parseLastAttributeId();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        if(isParentOf)
        {
            ddClass.parentId = id;
        }
    }

    
    private List<String> parseAttributeIds() throws Exception
    {
        List<String> list = new ArrayList<>(1);

        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            list.add(rd.nextString());
        }
        
        rd.endArray();

        return list;
    }

    
    private String parseLastAttributeId() throws Exception
    {
        String id = null;

        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            id = rd.nextString();
        }
        
        rd.endArray();

        // Remove registration authority
        if(id == null) return null;
        
        int idx = id.indexOf('.');
        if(idx < 0) throw new Exception("Invalid attributeId " + id);
        
        return id.substring(idx + 1);
    }

}
