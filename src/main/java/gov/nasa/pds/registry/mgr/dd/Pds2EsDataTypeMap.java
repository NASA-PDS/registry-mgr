package gov.nasa.pds.registry.mgr.dd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.Logger;


/**
 * <p>Mappings between PDS LDD data types such as 'ASCII_LID' 
 * and Elasticsearch data types such as 'keyword'.</p>
 * 
 * <p>Mappings are loaded from a configuration file similar to Java properties file.
 * There is one mapping per line:<br/>
 * &lt;PDS LDD data type&gt;=&lt;Elasticsearch data type&gt;</p>
 * 
 * <p>Default configuration file is in 
 * &lt;PROJECT_ROOT&gt;/src/main/resources/elastic/data-dic-types.cfg</p>
 * 
 * @author karpenko
 */
public class Pds2EsDataTypeMap
{
    private Map<String, String> map;
    
    /**
     * Constructor
     */
    public Pds2EsDataTypeMap()
    {
        map = new HashMap<>();
    }

    
    /**
     * Get Elasticsearch data type for a PDS LDD data type
     * @param pdsType PDS LDD data type
     * @return Elasticsearch data type
     */
    public String getEsType(String pdsType)
    {
        String esType = map.get(pdsType);
        if(esType != null) return esType;
        
        esType = guessType(pdsType);
        Logger.warn("No PDS to Elasticsearch data type mapping for '" + pdsType 
                + "'. Will use '" + esType + "'");

        map.put(pdsType, esType);
        return esType;
    }
    
    
    private String guessType(String str)
    {
        str = str.toLowerCase();
        if(str.contains("_real")) return "double";
        if(str.contains("_integer")) return "integer";
        if(str.contains("_string")) return "keyword";
        if(str.contains("_text")) return "text";
        if(str.contains("_date")) return "date";
        if(str.contains("_boolean")) return "boolean";        
        
        return "keyword";
    }
    
    
    /**
     * Load data type mappings from a configuration file
     * @param file Configuration file with PDS LDD to Elasticsearch data type mappings.
     * <p>Mappings are loaded from a configuration file similar to Java properties file.
     * There is one mapping per line:<br/>
     * &lt;PDS LDD data type&gt;=&lt;Elasticsearch data type&gt;</p>
     * @throws Exception
     */
    public void load(File file) throws Exception
    {
        if(file == null) return;
        
        Logger.info("Loading PDS to ES data type mapping from " + file.getAbsolutePath());
        
        BufferedReader rd = null;
        
        try
        {
            rd = new BufferedReader(new FileReader(file));
        }
        catch(Exception ex)
        {
            throw new Exception("Could not open data type configuration file '" + file.getAbsolutePath());
        }
        
        try
        {
            String line;
            while((line = rd.readLine()) != null)
            {
                line = line.trim();
                if(line.startsWith("#") || line.isEmpty()) continue;
                String[] tokens = line.split("=");
                if(tokens.length != 2) 
                {
                    throw new Exception("Invalid entry in data type configuration file " 
                            + file.getAbsolutePath() + ": " + line);
                }
                
                String key = tokens[0].trim();
                if(key.isEmpty()) 
                {
                    throw new Exception("Empty key in data type configuration file " 
                            + file.getAbsolutePath() + ": " + line);
                }
                
                String value = tokens[1].trim();
                if(key.isEmpty())
                {
                    throw new Exception("Empty value in data type configuration file " 
                            + file.getAbsolutePath() + ": " + line);
                }
                
                map.put(key, value);
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    /**
     * Prints all mappings
     */
    public void debug()
    {
        map.forEach((key, val) -> { System.out.println(key + "  -->  " + val); } );
    }
}
