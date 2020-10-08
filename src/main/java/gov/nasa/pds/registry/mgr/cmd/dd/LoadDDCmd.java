package gov.nasa.pds.registry.mgr.cmd.dd;

import java.io.File;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.DataLoader;
import gov.nasa.pds.registry.mgr.dd.parser.DDParser;
import gov.nasa.pds.registry.mgr.dd.DDProcessor;


public class LoadDDCmd implements CliCommand
{
    private String esUrl;
    private String indexName;
    private String authPath;
    
    
    public LoadDDCmd()
    {
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager load-dd <options>");

        System.out.println();
        System.out.println("Load data dictionary");
        System.out.println();        
        System.out.println("Required parameters, one of:");
        System.out.println("  -dd <path>         A data dictionary file (JSON)");
        System.out.println("  -dump <path>       A data dump created by export-dd command (NJSON)");        
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>       Authentication config file");
        System.out.println("  -es <url>          Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>      Elasticsearch index name. Default is 'registry'");        
        System.out.println("  -ns <namespaces>   Comma separated list of namespaces. Can be used with -dd parameter.");
        System.out.println();
    }

    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }

        this.esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        this.indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        this.authPath = cmdLine.getOptionValue("auth");

        String path = cmdLine.getOptionValue("dd");
        if(path != null)
        {
            String namespaces = cmdLine.getOptionValue("ns");
            loadDataDictionary(path, namespaces);
            return;
        }
        
        path = cmdLine.getOptionValue("dump");
        if(path != null)
        {
            loadDataDump(path);
            return;
        }        
        
        throw new Exception("One of the following options is required: -dd, -dump");
    }

        
    private void loadDataDictionary(String path, String namespaces) throws Exception
    {
        Set<String> nsFilter = new TreeSet<>();
        
        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println("  Data dictionary: " + path);
        
        if(namespaces != null)
        {
            System.out.println("       Namespaces: " + namespaces);
            String[] tokens = namespaces.split(",");
            for(String token: tokens)
            {
                token = token.trim();
                if(token.length() > 0)
                {
                    nsFilter.add(token);
                }
            }
        }
        System.out.println();
                
        // Parse data dictionary and create temporary file
        File tempOutFile = getTempOutFile();
        File dtCfgFile = getDataTypesCfgFile();
        
        DDProcessor proc = new DDProcessor(tempOutFile, dtCfgFile, nsFilter);
        DDParser parser = new DDParser();
        parser.parse(new File(path), proc);
        proc.processParentClasses();
        proc.close();
        
        // Load temporary file into data dictionary index
        DataLoader loader = new DataLoader(esUrl, indexName + "-dd", authPath);
        loader.loadFile(tempOutFile);
        
        // Delete temporary file
        tempOutFile.delete();
    }
    
    
    private void loadDataDump(String path) throws Exception
    {
        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println("        Data dump: " + path);        
        System.out.println();
        
        DataLoader loader = new DataLoader(esUrl, indexName + "-dd", authPath);
        loader.loadFile(new File(path));
    }
    
    
    private File getTempOutFile()
    {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File file = new File(tempDir, "pds-registry-dd.tmp.json");
        return file;
    }
    
    
    private File getDataTypesCfgFile() throws Exception
    {
        String home = System.getenv("REGISTRY_MANAGER_HOME");
        if(home == null) 
        {
            throw new Exception("Could not find default configuration directory. REGISTRY_MANAGER_HOME environment variable is not set.");
        }

        File file = new File(home, "elastic/data-dic-types.cfg");
        return file;
    }
}
