package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.schema.SchemaUpdater;
import gov.nasa.pds.registry.mgr.schema.dd.DDParser;
import gov.nasa.pds.registry.mgr.schema.dd.DDParser.DDAttr;
import gov.nasa.pds.registry.mgr.schema.dd.Pds2EsDataTypeMap;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;


public class UpdateSchemaCmd implements CliCommand
{
    private static class ParserCB implements DDParser.Callback
    {
        private Map<String, String> fields = new TreeMap<>();
        private Pds2EsDataTypeMap dtMap = new Pds2EsDataTypeMap();
        
        public ParserCB()
        {
        }
        
        @Override
        public void onAttribute(DDAttr attr) throws Exception
        {
            String tokens[] = attr.id.split("\\.");
            if(tokens.length != 5) throw new Exception("Invalid attribute ID " + attr.id);
            
            String fieldName = tokens[1] + "/" + tokens[2] + "/" + tokens[3] + "/" + tokens[4];
            String fieldType = dtMap.getEsType(attr.dataType);
            
            fields.put(fieldName, fieldType);
        }
        
        public Map<String, String> getFields()
        {
            return fields;
        }
    }
    
    
    public UpdateSchemaCmd()
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

        String ddFilePath = cmdLine.getOptionValue("file");
        if(ddFilePath == null) 
        {
            throw new Exception("Missing required parameter '-file'");
        }

        String esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        String indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        String authPath = cmdLine.getOptionValue("auth");

        // Read data dictionary
        Map<String, String> ddFields = getDDFields(ddFilePath);
        
        RestClient client = null;
        
        try
        {
            // Create Elasticsearch client
            client = EsUtils.createClient(esUrl, authPath);

            // Update Elasticsearch schema
            SchemaUpdater su = new SchemaUpdater(client, indexName);
            su.updateSchema(ddFields);
            
            System.out.println("Done");
        }
        catch(ResponseException ex)
        {
            throw new Exception(EsUtils.extractErrorMessage(ex));
        }
        finally
        {
            CloseUtils.close(client);
        }
    }

    
    private Map<String, String> getDDFields(String ddFilePath) throws Exception
    {
        File ddFile = new File(ddFilePath);
        System.out.println("Reading data dictionary " + ddFile.getAbsolutePath());
        
        ParserCB cb = new ParserCB();
        DDParser parser = new DDParser();        
        parser.parse(ddFile, cb);
        
        return cb.getFields();
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager update-schema <options>");

        System.out.println();
        System.out.println("Update Elasticsearch schema from one or more PDS data dictionaries.");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>     Data dictionary file (JSON).");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>     Authentication config file");
        System.out.println("  -es <url>        Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>    Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }
    
}
