package com.arturo.charlaSpark;

import com.arturo.charlaSpark.SparkInit.SparkInit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Example {
    public static final String BOOK = "book";
    public static final String CHAPTER = "chapter";
    public static final String VERSE = "verse";
    public static final String TEXT = "text";
    private static final String delimiters = "\\s+|,|;|:|\\.|\\?|!|-|\\(|\\)";
    
    public static void main(String[] args) {
        SparkSession spark = SparkInit.init();
        
        
        Dataset<Row> bible = spark.read().format("csv")
                .option("sep", "|")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("src/main/resources/bible.csv");
        
        bible.show();
        
        
        //Contar el número de versos que hay
        
        
        //Contar el número de versos que tiene cada libro
        
        
        //Ordenar alfabeticamente por la primera palabra de cada verso
        
        
        //Ver los versos que tienen la palabra Dios (God)
        
        
        //Número de versos que tienen la palabra Dios (God)
        
        
        //Contar el número de libros que hay
        
        
        //Contar el número de palabras que hay en cada verso
        
        
        //Contar el número de palabras que hay en total
        
        
        //Contar el número de palabras por libro
        
        
        //Ver el libro más largo
        

        // DIFICIL:
        // Contar el número de veces que aparece cada palabra
        
    }
}