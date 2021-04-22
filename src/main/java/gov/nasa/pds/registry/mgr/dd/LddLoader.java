package gov.nasa.pds.registry.mgr.dd;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import gov.nasa.pds.registry.mgr.dao.DataLoader;
import gov.nasa.pds.registry.mgr.dd.parser.AttributeDictionaryParser;
import gov.nasa.pds.registry.mgr.dd.parser.ClassAttrAssociationParser;
import gov.nasa.pds.registry.mgr.dd.parser.DDAttribute;


/**
 * Loads PDS LDD JSON file into Elasticsearch data dictionary index
 * 
 * @author karpenko
 */
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
 
    
    /**
     * Set Elasticsearch information
     * @param esUrl Elasticsearch URL, such as "http://localhost:9200"
     * @param indexName Elasticsearch base index name, such as "registry". 
     * NOTE: This class automatically creates full ES data dictionary index name, 
     * such as "registry-dd". Pass "base" index name.
     * @param authFilePath Path to optional authentication configuration file.
     */
    public void setElasticInfo(String esUrl, String indexName, String authFilePath)
    {
        this.esUrl = esUrl;
        this.esIndexName = indexName + "-dd";
        this.esAuthFilePath = authFilePath;
    }
    
    
    /**
     * Load PDS to Elasticsearch data type map
     * @param file
     * @throws Exception
     */
    public void loadPds2EsDataTypeMap(File file) throws Exception
    {
        dtMap.load(file);
    }
    
    
    /**
     * Load PDS LDD JSON file into Elasticsearch data dictionary index
     * @param ddFile PDS LDD JSON file
     * @param nsFilter Namespace filter. Only load classes having these namespaces.
     * @throws Exception
     */
    public void load(File ddFile, Set<String> nsFilter) throws Exception
    {
        File tempEsDataFile = new File(tempDir, "pds-registry-dd.tmp.json");
        
        // Parse and cache attributes
        Map<String, DDAttribute> ddAttrCache = new TreeMap<>();
        AttributeDictionaryParser attrParser = new AttributeDictionaryParser(ddFile, 
                (attr) -> { ddAttrCache.put(attr.id, attr); } );
        attrParser.parse();
        
        // Parse class attribute associations and write ES data file
        LddEsJsonWriter writer = new LddEsJsonWriter(tempEsDataFile, dtMap, ddAttrCache);
        writer.setNamespaceFilter(nsFilter);
        ClassAttrAssociationParser caaParser = new ClassAttrAssociationParser(ddFile, writer);
        caaParser.parse();
        writer.close();

        // Load temporary file into data dictionary index
        DataLoader loader = new DataLoader(esUrl, esIndexName, esAuthFilePath);
        loader.loadFile(tempEsDataFile);
        
        // Update data dictionary version
        
        // Delete temporary file
        tempEsDataFile.delete();
    }

}
