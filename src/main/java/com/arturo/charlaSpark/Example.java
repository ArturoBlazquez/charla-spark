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
        System.out.println(bible.count());
        
        //Contar el número de versos que tiene cada libro
        bible.groupBy(col(BOOK)).count().show();
        
        //Ordenar alfabeticamente por la primera palabra de cada verso
        bible.orderBy(col(TEXT)).show();
        
        //Ver los versos que tienen la palabra Dios (God)
        bible.filter(col(TEXT).contains("God")).show();
        
        //Número de versos que tienen la palabra Dios (God)
        System.out.println(bible.filter(col(TEXT).contains("God")).count());
        
        bible.groupBy(col(TEXT).like("%God%")).count().show();
        
        //Contar el número de libros que hay
        System.out.println(bible.select(col(BOOK)).distinct().count());
        
        //Contar el número de palabras que hay en cada verso
        bible.withColumn("palabras_por_verso", size(split(col(TEXT), delimiters))).show();
        
        //Contar el número de palabras que hay en total
        Dataset<Row> versesWithCount = bible.withColumn("palabras_por_verso", size(split(col(TEXT), delimiters)));
        versesWithCount.agg(sum(col("palabras_por_verso")).as("palabras_totales")).show();
        
        //Contar el número de palabras por libro
        versesWithCount.groupBy(BOOK).agg(sum(col("palabras_por_verso")).as("palabras_por_libro")).show();
        
        //Ver el libro más largo
        Dataset<Row> bookWithCount = versesWithCount.groupBy(BOOK).agg(sum(col("palabras_por_verso")).as("palabras_por_libro"));
        bookWithCount.groupBy(col(BOOK)).agg(max("palabras_por_libro")).show();
        
        // DIFICIL:
        // Contar el número de veces que aparece cada palabra
        bible.withColumn("word", explode(split(lower(col(TEXT)), delimiters)))
                .groupBy(col("word")).count().orderBy(desc("count")).show();
    }
}