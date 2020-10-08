package gov.nasa.pds.registry.mgr.dd.parser;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.mgr.dd.parser.DDParser.Callback;
import gov.nasa.pds.registry.mgr.dd.parser.DDAttribute;


class DDAttrParser
{
    private JsonReader rd;
    private Callback cb;
    private int itemCount;

    
    public DDAttrParser(JsonReader rd, Callback cb)
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


    private void parseAttr() throws Exception
    {
        itemCount++;
        
        DDAttribute attr = new DDAttribute();
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                String id = rd.nextString();
                String tokens[] = id.split("\\.");
                if(tokens.length != 5) throw new Exception("Could not parse attribute id " + id);
                
                attr.classNs = tokens[1];
                attr.className = tokens[2];
                attr.attrNs = tokens[3];
                attr.attrName = tokens[4];
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
        
        if(attr.attrName == null)
        {
            String msg = "Missing identifier in attribute definition. Index = " + itemCount;
            throw new Exception(msg);
        }
        
        if(attr.dataType == null) 
        {
            String msg = "Missing dataType in attribute definition " + attr.getId();
            throw new Exception(msg); 
        }

        cb.onAttribute(attr);
    }

}
