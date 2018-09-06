/*
 * Fuente de los datos: https://www.kaggle.com/c/sf-crime/data.
 *
 * Dataset de incidentes atendidos por la policía de San Francisco de 2003 al 2015.
 */

package com.arturo.charlaSpark;

import com.arturo.charlaSpark.SparkInit.SparkInit;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class MLExample {
    
    public static void main(String[] args) {
        SparkSession spark = SparkInit.init();
        
        Dataset<Row> data = spark.read().load("src/main/resources/data");
        
        data = data.select(col("Descript"), col("Category"));
        
        data.show(30, false);
        System.out.println("Hay " + data.count() + " filas de datos");
        
        data = process(data);
        
        data.show();
        
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.8, 0.2}, System.currentTimeMillis());
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];
        
        
        //Definimos tres clasificadores diferentes
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(20)
                .setRegParam(0.3)
                .setElasticNetParam(0);
        
        NaiveBayes nb = new NaiveBayes()
                .setSmoothing(1);
        
        RandomForestClassifier forest = new RandomForestClassifier()
                .setMaxDepth(4)
                .setMaxBins(32)
                .setNumTrees(20);
        
        
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");
        
        //Entrenamos cada uno de los modelos y clasificamos los datos
        LogisticRegressionModel lrModel = lr.fit(training);
        Dataset<Row> lrPredictions = lrModel.transform(test);
        
        NaiveBayesModel nbModel = nb.fit(training);
        Dataset<Row> nbPredictions = nbModel.transform(test);
        
        RandomForestClassificationModel forestModel = forest.fit(training);
        Dataset<Row> forestPredictions = forestModel.transform(test);
        
        
        //Vemos la precisión de cada uno de los modelos
        System.out.printf("\n\nPorcentaje de error de logistic regression: %.2f%%\n", (1.0 - evaluator.evaluate(lrPredictions)) * 100);
        System.out.printf("Porcentaje de error de Naive Bayes: %.2f%%\n", (1.0 - evaluator.evaluate(nbPredictions)) * 100);
        System.out.printf("Porcentaje de error de random forest: %.2f%%", (1.0 - evaluator.evaluate(forestPredictions)) * 100);
    }
    
    private static Dataset<Row> process(Dataset<Row> data) {
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("Descript")
                .setOutputCol("words")
                .setPattern("\\W");
        
        String[] add_stopwords = new String[]{"http", "https", "amp", "rt", "t", "c", "the"};
        StopWordsRemover stopWordsRemover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered")
                .setStopWords(add_stopwords);
        
        CountVectorizer countVectorizer = new CountVectorizer()
                .setInputCol("filtered")
                .setOutputCol("features")
                .setVocabSize(10000)
                .setMinDF(5);
        
        StringIndexer stringIndexer = new StringIndexer()
                .setInputCol("Category")
                .setOutputCol("label");
        
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{regexTokenizer, stopWordsRemover, countVectorizer, stringIndexer});
        
        return pipeline.fit(data).transform(data);
    }
}
