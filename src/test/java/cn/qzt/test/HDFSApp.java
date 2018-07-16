package cn.qzt.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * Title:  hadoopdemo
 * Description:  SSD
 *
 * @author dc
 * @date2018/7/919:10
 */
public class HDFSApp {
    private static final String HDFS_PATH="hdfs://192.168.1.133:8020";


    Configuration configuration=null;
    FileSystem fileSystem=null;
    @Before
    public void setUp()throws Exception{
        System.out.println("HDFSApp.setUP()");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");

    }

    /**
     * 创建目录
     * @throws Exception
     */
    @Test
    public void mkdir()throws Exception{

        fileSystem.mkdirs(new Path("/hdfsapi/test"));

    }

    /**
     * 创建文件并写入
     * @throws Exception
     */
    @Test
    public void create()throws Exception{

        FSDataOutputStream output = fileSystem.create(new Path("hadfsapi/test/a.txt"));
        output.write("hello hadoop!".getBytes());
        output.close();

    }

    /**
     * 上传文件到Hadoop
     * @throws Exception
     */
    @Test
    public void copyFormLocalFile()throws Exception{
        Path src = new Path("E:\\安装包\\hadoop-2.6.0-cdh5.7.0.tar.gz");
        Path dist = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(src,dist);

    }
    @After
    public void tearDown(){
    fileSystem=null;
    configuration=null;
        System.out.println("HDFSApp.tearDown()");


    }
}
