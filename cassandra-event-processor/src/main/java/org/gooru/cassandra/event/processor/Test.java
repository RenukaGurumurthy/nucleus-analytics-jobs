package org.gooru.cassandra.event.processor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {

    public static void main(String[] args) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
        Long startTime = null;
        
        System.out.println(dateFormatter.format(new Date(1518411803466l)));

 /*       try {
            startTime = dateFormatter.parse("1499295246965").getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Long endTime = new Date().getTime();
        System.out.println("starting startTime:" + startTime + "  endTime:" + endTime);
        
        if (startTime >= endTime) {
            System.out.println("start time is greater than endtime, nothing to process, aborting..");
            System.exit(0);
        } 

        while(startTime < endTime) {
            System.out.println("processing time:" + dateFormatter.format(new Date(startTime)));
            startTime = new Date(startTime).getTime() + 60000;
        }
*/
    }

}
