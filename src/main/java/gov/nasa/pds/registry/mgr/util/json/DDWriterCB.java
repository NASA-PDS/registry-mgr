package gov.nasa.pds.registry.mgr.util.json;

import java.io.File;
import java.util.Set;

import gov.nasa.pds.registry.mgr.util.dd.DDAttribute;
import gov.nasa.pds.registry.mgr.util.dd.DDNJsonWriter;
import gov.nasa.pds.registry.mgr.util.dd.DDParser;
import gov.nasa.pds.registry.mgr.util.dd.Pds2EsDataTypeMap;


public class DDWriterCB implements DDParser.Callback
{
    private DDNJsonWriter writer;
    private DDAttribute attr = new DDAttribute();
    private Pds2EsDataTypeMap dtMap;
    private Set<String> nsFilter;
    
    
    public DDWriterCB(File ddFile, File typeMapFile, Set<String> nsFilter) throws Exception
    {
        if(nsFilter != null && nsFilter.size() > 0)
        {
            this.nsFilter = nsFilter;
        }

        dtMap = new Pds2EsDataTypeMap();
        if(typeMapFile != null)
        {
            dtMap.load(typeMapFile);
        }
        
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
        
        // Apply namespace filter
        if(nsFilter != null && !nsFilter.contains(tokens[1])) return;        
        
        // Assign values
        attr.classNs = tokens[1];
        attr.className = tokens[2];
        attr.attrNs = tokens[3];
        attr.attrName = tokens[4];
        
        attr.esFieldName = attr.classNs + "/" + attr.className 
                + "/" + attr.attrNs + "/" + attr.attrName;
        
        attr.dataType = dda.dataType;
        attr.esDataType = dtMap.getEsType(attr.dataType);
        
        attr.description = dda.description;

        // Write
        writer.write(attr.esFieldName, attr);
    }

    
    public void close() throws Exception
    {
        writer.close();
    }
}
