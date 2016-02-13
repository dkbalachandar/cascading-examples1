package com.cascading;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

import java.util.Properties;

public class HdfsFileCopy {

    public static void main(String[] args) {

        System.out.println("Hdfs Copy Job is started");
        //input and output path
        String inputPath = args[0];
        String outputPath = args[1];
        System.out.println("inputPath::" + inputPath);
        System.out.println("outputPath::" + outputPath);

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, HdfsFileCopy.class);
        //input tap[Source]
        Tap inTap = new Hfs(new TextLine(), inputPath);
        //Sink tap
        Tap outTap = new Hfs(new TextLine(), outputPath, SinkMode.REPLACE);
        // Pipe to connect Source and Sink Tap
        Pipe copyPipe = new Pipe("copy");
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
        Flow flow = flowConnector.connect("File Copy", inTap, outTap, copyPipe);
        flow.complete();
        System.out.println("Hdfs Copy  is completed");
    }
}
