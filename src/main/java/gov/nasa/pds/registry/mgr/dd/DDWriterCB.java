package gov.nasa.pds.registry.mgr.dd;

import java.io.File;
import java.util.Set;

import gov.nasa.pds.registry.mgr.dd.parser.DDAttribute;
import gov.nasa.pds.registry.mgr.dd.parser.DDClass;
import gov.nasa.pds.registry.mgr.dd.parser.DDParser;


public class DDWriterCB implements DDParser.Callback
{
    private DDNJsonWriter writer;
    private DDRecord ddrec = new DDRecord();
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
    public void onClass(DDClass claz) throws Exception
    {
    }

    
    @Override
    public void onAttribute(DDAttribute dda) throws Exception
    {
        // Apply namespace filter
        if(nsFilter != null && !nsFilter.contains(dda.classNs)) return;        
        
        // Assign values
        ddrec.classNs = dda.classNs;
        ddrec.className = dda.className;
        ddrec.attrNs = dda.attrNs;
        ddrec.attrName = dda.attrName;
        
        ddrec.dataType = dda.dataType;
        ddrec.esDataType = dtMap.getEsType(dda.dataType);
        
        ddrec.description = dda.description;

        // Write
        writer.write(ddrec.getEsFieldName(), ddrec);
    }

    
    public void close() throws Exception
    {
        writer.close();
    }

}
