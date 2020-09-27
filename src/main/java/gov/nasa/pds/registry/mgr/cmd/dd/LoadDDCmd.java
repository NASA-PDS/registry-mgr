package gov.nasa.pds.registry.mgr.cmd.dd;

import java.io.File;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.schema.dd.DDParser;
import gov.nasa.pds.registry.mgr.schema.dd.Pds2EsDataTypeMap;


public class LoadDDCmd implements CliCommand
{
    private static class DDWriterCB implements DDParser.Callback
    {
        private DDNJsonWriter writer;
        private DDAttribute attr = new DDAttribute();
        private Pds2EsDataTypeMap dtMap;
        
        
        public DDWriterCB(File ddFile) throws Exception
        {
            dtMap = new Pds2EsDataTypeMap();
            //dtMap.load(file);
            
            writer = new DDNJsonWriter(ddFile);
        }
        
        
        @Override
        public void onAttribute(DDParser.DDAttr dda) throws Exception
        {
            String tokens[] = dda.id.split("\\.");
            if(tokens.length != 5)
            {
                System.out.println("[WARNING] Invalid attribute ID " + dda.id);
                return;
            }
            
            attr.classNs = tokens[1];
            attr.className = tokens[2];
            attr.attrNs = tokens[3];
            attr.attrName = tokens[4];
            
            attr.esFieldName = attr.classNs + "/" + attr.className 
                    + "/" + attr.attrNs + "/" + attr.attrName;
            
            attr.dataType = dda.dataType;
            attr.esDataType = dtMap.getEsType(attr.dataType);
            
            attr.description = dda.description;
            
            writer.write(attr.esFieldName, attr);
        }

        
        public void close() throws Exception
        {
            writer.close();
        }
    }
    

    public LoadDDCmd()
    {
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager load-dd <options>");

        System.out.println();
        System.out.println("Load data dictionary");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>    A data dictionary file (JSON) to load."); 
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>    Authentication config file");
        System.out.println("  -es <url>       Elasticsearch URL. Default is http://localhost:9200");
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

        String esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        String authPath = cmdLine.getOptionValue("auth");

        String ddFile = cmdLine.getOptionValue("file");
        if(ddFile == null) 
        {
            throw new Exception("Missing required parameter '-file'");
        }

        System.out.println("  Data dictionary: " + ddFile);
        System.out.println("Elasticsearch URL: " + esUrl);
        
        System.out.println();
        System.out.println("Reading data dictionary...");
        
        DDParser parser = new DDParser();
        DDWriterCB wr = new DDWriterCB(new File("/tmp/njson.json"));
        parser.parse(new File(ddFile), wr);
        wr.close();
    }

}
