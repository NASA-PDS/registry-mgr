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
    
    
    public static String extractVersionFromUrl(String url) throws Exception
    {
        url = url.toLowerCase();
        if(!url.endsWith(".json")) throw new Exception("Invalid data dictionary URL. Not a JSON file: " + url);
            
        return null;
    }
}
