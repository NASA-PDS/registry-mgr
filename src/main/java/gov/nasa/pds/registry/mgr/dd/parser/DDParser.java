package gov.nasa.pds.registry.mgr.dd.parser;

import java.io.File;
import java.io.FileReader;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.mgr.util.CloseUtils;


public class DDParser
{
    public static interface Callback
    {
        public void onClass(DDClass claz) throws Exception;
        public void onAttribute(DDAttribute attr) throws Exception;
    }
    
    // ----------------------------------------------------------------
    
    private JsonReader rd;
    private DDClassParser classParser;
    private DDAttrParser attrParser;
    
    private boolean parseAttrDic = true;
    private boolean parseClassDic = true;
    
    
    public DDParser() throws Exception
    {
    }

    
    public void setParseClassDictionary(boolean b)
    {
        this.parseClassDic = b;
    }

    
    public void setParseAttributeDictionary(boolean b)
    {
        this.parseAttrDic = b;
    }
    
        
    private void init(File file, Callback cb) throws Exception
    {
        this.rd = new JsonReader(new FileReader(file));
        classParser = new DDClassParser(rd, cb);
        attrParser = new DDAttrParser(rd, cb);
    }
    
    
    public void parse(File file, Callback cb) throws Exception
    {
        init(file, cb);

        try
        {
            parseRoot();
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    private void parseRoot() throws Exception
    {
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
    }
    
    
    private void parseDataDic() throws Exception
    {
        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            
            if("classDictionary".equals(name))
            {
                if(parseClassDic)
                {
                    classParser.parse();
                }
                else 
                {
                    rd.skipValue();
                }
            }
            else if("attributeDictionary".equals(name))
            {
                if(parseAttrDic)
                {
                    attrParser.parse();
                }
                else 
                {
                    rd.skipValue();
                }
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }

}
