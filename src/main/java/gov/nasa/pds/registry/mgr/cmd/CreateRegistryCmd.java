package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;
import java.net.URL;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.mgr.Constants;


public class CreateRegistryCmd implements CliCommand
{
    public CreateRegistryCmd()
    {
    }
    
    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }

        String esUrl = cmdLine.getOptionValue("url", "http://localhost:9200");
        File schemaFile = getSchemaFile(cmdLine.getOptionValue("path"));
        
        String collectionName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        int shards = parseShards(cmdLine.getOptionValue("shards", "1"));
        int replicas = parseReplicas(cmdLine.getOptionValue("replicas", "0"));
        
        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("           Schema: " + schemaFile.getAbsolutePath());
        System.out.println("            Index: " + collectionName);
        System.out.println("           Shards: " + shards);
        System.out.println("         Replicas: " + replicas);
        System.out.println();

        try
        {
            System.out.println("Creating collection...");
            
            
            System.out.println("Done");
        }
        finally
        {

        }
    }

    
    private int parseShards(String str) throws Exception
    {
        int val = parseInt(str);
        if(val <= 0) throw new Exception("Invalid number of shards: " + str);
        
        return val;
    }
    

    private int parseReplicas(String str) throws Exception
    {
        int val = parseInt(str);
        if(val < 0) throw new Exception("Invalid number of replicas: " + str);
        
        return val;
    }

    
    private int parseInt(String str)
    {
        if(str == null) return 0;
        
        try
        {
            return Integer.parseInt(str);
        }
        catch(Exception ex)
        {
            return -1;
        }
    }
    
    
    private File getSchemaFile(String path) throws Exception
    {
        File file = null;
        
        if(path == null)
        {
            // Get default
            String home = System.getenv("REGISTRY_MANAGER_HOME");
            if(home == null) 
            {
                throw new Exception("Could not find default configuration directory. REGISTRY_MANAGER_HOME environment variable is not set.");
            }
            
            file = new File(home, "elastic/registry.json");
        }
        else
        {
            file = new File(path);
        }
        
        if(!file.exists()) throw new Exception("Schema file " + file.getAbsolutePath() + " does not exist");
        
        return file;
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager create-registry <options>");

        System.out.println();
        System.out.println("Create registry collection");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -url <url>           Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>        Elasticsearch index name. Default value is 'registry'");
        System.out.println("  -schema <path>       Elasticsearch index schema (JSON file)"); 
        System.out.println("                       Default value is $REGISTRY_MANAGER_HOME/elastic/registry.json");
        System.out.println("  -shards <number>     Number of shards (partitions) for registry index. Default is 1");
        System.out.println("  -replicas <number>   Number of replicas (extra copies) of registry index. Default is 0");
        System.out.println();
    }

}
