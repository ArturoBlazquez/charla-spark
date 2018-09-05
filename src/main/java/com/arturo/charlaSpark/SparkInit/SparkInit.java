package com.arturo.charlaSpark.SparkInit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class SparkInit {
    
    public static SparkSession init() {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        
        Logger.getLogger("org.apache.spark.sql.execution").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark.sql.catalyst.expressions.codegen").setLevel(Level.FATAL);
        
        showLogo();
        
        SparkSession spark = SparkSession.builder().appName("db-example").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        
        return spark;
    }
    
    private static void showLogo() {
        System.out.println();
        System.out.println("      ____              __");
        System.out.println("     / __/__  ___ _____/ /__");
        System.out.println("    _\\ \\/ _ \\/ _ `/ __/  '_/");
        System.out.println("   /___/ .__/\\_,_/_/ /_/\\_\\");
        System.out.println("      /_/");
        System.out.println();
        System.out.println();
    }
}
