package gov.nasa.pds.registry.mgr.util;


import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;


public class EsUtils
{
    private static final String ERR_FIELD_NAMES = "Could not get list of fields";
    
    
    public static Set<String> getFieldNames(RestClient client, String indexName) throws Exception
    {
        Request req = new Request("GET", "/" + indexName + "/_mappings");
        Response resp = client.performRequest(req);
        
        InputStream is = resp.getEntity().getContent();
        JsonReader rd = new JsonReader(new InputStreamReader(is));
        
        Set<String> set = new TreeSet<>();
        
        rd.beginObject();
        if(!indexName.equals(rd.nextName())) 
        {
            rd.close();
            throw new Exception(ERR_FIELD_NAMES); 
        }

        rd.beginObject();
        if(!"mappings".equals(rd.nextName())) 
        {
            rd.close();
            throw new Exception(ERR_FIELD_NAMES); 
        }
        
        rd.beginObject();
        if(!"properties".equals(rd.nextName()))
        {
            rd.close();
            throw new Exception(ERR_FIELD_NAMES);
        }

        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            rd.skipValue();
            set.add(name);
        }
        
        rd.close();
        
        return set;
    }
    
    
    public static void updateMappings(RestClient client, String indexName, String json) throws Exception
    {
        
    }
    
    
    public static String extractErrorMessage(ResponseException ex)
    {
        String msg = ex.getMessage();
        if(msg == null) return "Unknown error";
        
        String lines[] = msg.split("\n");
        if(lines.length < 2) return msg;
        
        String reason = extractReasonFromJson(lines[1]);
        if(reason == null) return msg;
        
        return reason;
    }
    
    
    @SuppressWarnings("rawtypes")
    private static String extractReasonFromJson(String json)
    {
        try
        {
            Gson gson = new Gson();
            Object obj = gson.fromJson(json, Object.class);
            
            obj = ((Map)obj).get("error");
            obj = ((Map)obj).get("reason");
            
            return obj.toString();
        }
        catch(Exception ex)
        {
            return null;
        }
    }

    
    public static void printWarnings(Response resp)
    {
        List<String> warnings = resp.getWarnings();
        if(warnings != null)
        {
            for(String warn: warnings)
            {
                System.out.println("[WARN] " + warn);
            }
        }
    }
}
