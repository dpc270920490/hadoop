package com.qzt.hadoop;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


import java.util.Date;

/**
 * Title:  hadoopdemo
 * Description:  SSD
 *
 * @author dc
 * @date2018/7/1111:40
 */
public class LogGenerator {
    public static void main(String[] args) throws InterruptedException {
        Logger logger = LogManager.getLogger("LogGenerator");

        int i = 0;
        while (true){
            logger.info("~~~~~"+new Date().toString()+"~~~~~~~");
            i++;
            Thread.sleep(500);
            if (i>5000000) {
                break;
            }
        }

    }
}
