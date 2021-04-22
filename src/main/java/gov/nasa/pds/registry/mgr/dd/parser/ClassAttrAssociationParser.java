package gov.nasa.pds.registry.mgr.dd.parser;

import java.io.File;
import com.google.gson.stream.JsonToken;


/**
 * PDS LDD JSON file parser. 
 * Parses "dataDictionary" -> "classDictionary" subtree and extracts attribute associations 
 * ("class" -> "association" -> "isAttribute" == true).
 * For each "attributeId" a callback method is called.
 * 
 * @author karpenko
 */
public class ClassAttrAssociationParser extends BaseDDParser
{
    /**
     * Callback interface 
     * @author karpenko
     */
    public static interface Callback
    {
        /**
         * This method is called for each "attributeId" from class attribute association
         * ("class" -> "association" -> "isAttribute" == true).
         * @param classNs class namespace
         * @param className class name
         * @param attrId attribute ID
         * @throws Exception
         */
        public void onAssociation(String classNs, String className, String attrId) throws Exception;
    }
    
    ////////////////////////////////////////////////////////////////////////
    
    
    private Callback cb;
    private int itemCount;

    private String classNs;
    private String className;
    
    
    /**
     * Constructor
     * @param file PDS LDD JSON file
     * @param cb Callback
     * @throws Exception
     */
    public ClassAttrAssociationParser(File file, Callback cb) throws Exception
    {
        super(file);
        this.cb = cb;
    }

    
    @Override
    protected void parseClassDictionary() throws Exception
    {
        System.out.println("[INFO] Parsing class and attribute associations...");

        jsonReader.beginArray();
        
        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_ARRAY)
        {
            jsonReader.beginObject();

            while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_OBJECT)
            {
                String name = jsonReader.nextName();
                if("class".equals(name))
                {
                    parseClass();
                }
                else
                {
                    jsonReader.skipValue();
                }
            }
            
            jsonReader.endObject();
        }
        
        jsonReader.endArray();
    }


    private void parseClass() throws Exception
    {
        itemCount++;
        classNs = null;
        className = null;
        
        jsonReader.beginObject();
        
        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_OBJECT)
        {
            String name = jsonReader.nextName();
            if("identifier".equals(name))
            {
                String id = jsonReader.nextString();
                
                String tokens[] = id.split("\\.");
                if(tokens.length != 3) throw new Exception("Could not parse class id " + id);
                
                classNs = tokens[1];
                className = tokens[2];
            }
            else if("associationList".equals(name))
            {
                parseAssocList();
            }
            else
            {
                jsonReader.skipValue();
            }
        }
        
        jsonReader.endObject();
        
        if(className == null)
        {
            String msg = "Missing identifier in class definition. Index = " + itemCount;
            throw new Exception(msg);
        }
    }


    private void parseAssocList() throws Exception
    {
        jsonReader.beginArray();
        
        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_ARRAY)
        {
            jsonReader.beginObject();

            while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_OBJECT)
            {
                String name = jsonReader.nextName();
                if("association".equals(name))
                {
                    parseAssoc();
                }
                else
                {
                    jsonReader.skipValue();
                }
            }
            
            jsonReader.endObject();
        }
        
        jsonReader.endArray();
    }

    
    private void parseAssoc() throws Exception
    {
        boolean isAttribute = false;
        
        jsonReader.beginObject();
        
        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_OBJECT)
        {
            String name = jsonReader.nextName();
            if("isAttribute".equals(name))
            {
                String val = jsonReader.nextString();
                if("true".equals(val))
                {
                    isAttribute = true;
                }
            }
            else if("attributeId".equals(name) && isAttribute)
            {
                parseAttributeIds();
            }
            else
            {
                jsonReader.skipValue();
            }
        }
        
        jsonReader.endObject();
    }

    
    private void parseAttributeIds() throws Exception
    {
        jsonReader.beginArray();
        
        while(jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_ARRAY)
        {
            String attrId = jsonReader.nextString();
            cb.onAssociation(classNs, className, attrId);
        }
        
        jsonReader.endArray();
    }
    
}
