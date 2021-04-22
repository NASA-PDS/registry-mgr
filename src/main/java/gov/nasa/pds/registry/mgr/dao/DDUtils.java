package gov.nasa.pds.registry.mgr.dao;

import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.TreeMap;

import com.opencsv.CSVReader;

import gov.nasa.pds.registry.common.util.CloseUtils;


public class DDUtils
{
    public static Map<String, DDInfo> createDataDictionaryMap(File file) throws Exception
    {
        CSVReader rd = null;
        
        try
        {
            rd = new CSVReader(new FileReader(file));
            
            Map<String, DDInfo> map = new TreeMap<>();
            
            String[] values;
            int lineNum = 0;
            while((values = rd.readNext()) != null)
            {
                lineNum++;
                
                if(values.length == 2 || values.length == 3)
                {
                    // Namespace
                    if(values[0] == null || values[0].isBlank()) 
                    {
                        throw new Exception("Line " + lineNum +  ": Missing namespace.");
                    }
                    
                    String ns = values[0].trim();
                    
                    // URL and latest version
                    if(values[1] == null || values[1].isBlank()) 
                    {
                        throw new Exception("Line " + lineNum +  ": Data dictionary URL for '" 
                                + ns + "' namespace is empty.");
                    }
                    
                    DDInfo info = new DDInfo();
                    info.url = values[1].trim();
                    info.version = extractVersionFromUrl(info.url);
                    
                    map.put(ns, info);
                }
            }
            
            return map;
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    public static long extractVersionFromUrl(String url) throws Exception
    {
        // Validate HTTP / HTTPS protocol
        String lowerUrl = url.toLowerCase();
        if(!lowerUrl.startsWith("http://") && !lowerUrl.startsWith("https://")) 
            throw new Exception("Invalid data dictionary URL. Only HTTP and HTTPS protocols are supported: " + url);

        // Extract file name
        int idx = lowerUrl.lastIndexOf('/');
        String fileName = lowerUrl.substring(idx+1);
        
        // Validate supported file types
        if(fileName.endsWith(".json"))
        {
            fileName = fileName.substring(0, fileName.length() - 5);
        }
        else
        {
            throw new Exception("Invalid data dictionary URL. Not a JSON file: " + url);            
        }
        
        // Extract version
        String[] tokens = fileName.split("_");
        if(tokens.length < 2)
            throw new Exception("Could not extract data dictionary version from " + url);
        
        int verLo = parseHexInt(tokens[tokens.length - 1]);
        if(verLo < 0)
            throw new Exception("Could not extract data dictionary version from " + url);
        
        int verHi = parseHexInt(tokens[tokens.length - 2]);
        if(verHi < 0)
        {
            return verLo;
        }

        return  parseHexInt(tokens[tokens.length - 2] + tokens[tokens.length - 1]);
    }
    
    
    private static int parseHexInt(String str)
    {
        try
        {
            return Integer.parseInt(str, 16);
        }
        catch(Exception ex)
        {
            return -1;
        }
    }
}
