package tt;

import java.io.File;

import gov.nasa.pds.registry.mgr.util.file.FileDownloader;

public class TestFileUtils
{

    public static void main(String[] args) throws Exception
    {
        FileDownloader utils = new FileDownloader();
        utils.download("https://192.168.0.4:8443/test.csv", new File("/tmp/test.csv"));
        
        System.out.println("Done");
    }

}
