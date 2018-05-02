package com.bim.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class HadoopDemo1{

    private static Configuration configuration = new Configuration();
    private static FileSystem fs;
    static{
        try{
            configuration.set("dfs.replication","1");
            fs = FileSystem.get(new URI("hdfs://cor1:9000"),configuration);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 源码跟踪
     */
    @Test
    public void create(){
        try {
            fs.create(new Path("/test9.log"));
        } catch (IOException e) {

        }
    }
    
    /**
     * 创建目录
     */
    @Test
    public void mkdir(){
        try{
            String pathString = "/test";
            if(!fs.exists(new Path(pathString))){
                boolean result = fs.mkdirs(new Path(pathString));
                System.out.println(result);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * 删除文件
     */
    @Test
    public void delFile(){

        try{

            Path dst = new Path("/backup/20180426/");

            fs.deleteOnExit(dst);

        }catch (Exception e){
            e.printStackTrace();
        }

    }


    /**
     * 下载到本地
     */
    @Test
    public void moveFile(){

        try{

            Path src = new Path("/test/demo1.txt");

            FSDataInputStream in = fs.open(src);
            FileOutputStream out = new FileOutputStream("/home/zyh/demo1.txt");
            IOUtils.copy(in,out);

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * copy from local file to HDFS file
     */
    @Test
    public void copyFileToHdfs1(){

        try{

            Path src = new Path("/home/zyh/Documents/bim-backupdata/modeldata");
            Path dst = new Path("/backup/20180426/modeldata");

            fs.copyFromLocalFile(src, dst);

        }catch (Exception e){
            e.printStackTrace();
        }

    }


    /**
     * 查找/目录下所有目录以及文件
     */
    @Test
    public void selectFileAndDocument1(){

        try{

            Path dst = new Path("/backup/20180426");
/*
            RemoteIterator<Path> pathRemoteIterator = fs.listCorruptFileBlocks(dst);
            while(pathRemoteIterator.hasNext()){
                Path next = pathRemoteIterator.next();
                System.out.println(next.getName()+":"+next.toString());
            }
            */
            RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(dst);
            while(iterator.hasNext()){
                LocatedFileStatus next = iterator.next();
                System.out.println(next.getPath().getName()+":"+ next.isDirectory());
            }


        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * 查找/目录下所有目录以及文件
     */
    @Test
    public void selectFileAndDocument2(){

        try{

            Path dst = new Path("/");
            FileStatus[] fileStatuses = fs.listStatus(dst);
            for (FileStatus fileStatus : fileStatuses){
                System.out.println(fileStatus.getPath());
            }

        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
