package gov.nasa.pds.registry.mgr.dd;

import java.io.File;
import java.util.Set;

import gov.nasa.pds.registry.mgr.dao.DataLoader;
import gov.nasa.pds.registry.mgr.dd.parser.AttributeDictionaryParser;
import gov.nasa.pds.registry.mgr.dd.parser.ClassAttrAssociationParser;


public class LddLoader
{
    private String esUrl = "http://localhost:9200";
    private String esIndexName = "registry-dd";
    private String esAuthFilePath;

    private File tempDir;
    private Pds2EsDataTypeMap dtMap;
    
    
    /**
     * Constructor
     */
    public LddLoader()
    {
        tempDir = new File(System.getProperty("java.io.tmpdir"));
        dtMap = new Pds2EsDataTypeMap();
    }
 
    
    public void setElasticInfo(String esUrl, String indexName, String authFilePath)
    {
        this.esUrl = esUrl;
        this.esIndexName = indexName + "-dd";
        this.esAuthFilePath = authFilePath;
    }
    
    
    public void loadPds2EsDataTypeMap(File file) throws Exception
    {
        dtMap.load(file);
    }
    
    
    public void load(File ddFile, Set<String> nsFilter) throws Exception
    {
        File tempEsDataFile = new File(tempDir, "pds-registry-dd.tmp.json");
        
        LddProcessor proc = new LddProcessor(tempEsDataFile, dtMap, nsFilter);
        
        AttributeDictionaryParser parser1 = new AttributeDictionaryParser(ddFile, proc);
        parser1.parse();
        ClassAttrAssociationParser parser2 = new ClassAttrAssociationParser(ddFile, proc);
        parser2.parse();
        proc.close();

        // Load temporary file into data dictionary index
        DataLoader loader = new DataLoader(esUrl, esIndexName, esAuthFilePath);
        loader.loadFile(tempEsDataFile);
        
        // Delete temporary file
        tempEsDataFile.delete();

    }

}
