
package com.cascading;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;

import java.util.Properties;


public class LocalFileCopy {

    public static void main(String[] args) {

        //input and output path
        String inputPath = args[0];
        String outputPath = args[1];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, LocalFileCopy.class);
        // create the source tap
        Tap inTap = new FileTap(new TextLine(), inputPath);
        // create the sink tap
        Tap outTap = new FileTap(new TextLine(), outputPath, SinkMode.REPLACE);
        // specify a pipe to connect the taps
        Pipe copyPipe = new Pipe("copy");

        LocalFlowConnector flowConnector = new LocalFlowConnector(properties);
        Flow flow = flowConnector.connect("File Copy", inTap, outTap, copyPipe);
        flow.complete();
    }

}

