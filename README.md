# DSE230_Data_Analysis_Using_Hadoop_and_Spark_UCSD
Map-reduce, streaming analysis, and external memory algorithms and their implementation using the Hadoop and its eco-system: HBase, Hive, Pig and Spark. The class will include assignment of analyzing large existing databases.

  <h2 id="section-1-distributed-computation-using-map-reduce">Section 1: Distributed computation using Map Reduce</h2>
<ul>
  <li>map-reduce</li>
  <li>counting words example, loading, processing, collecting.</li>
  <li>The work environment: Notebooks, markdown, code cells, display cells, S3, passwords and Vault, github.</li>
  <li>the memory hierarchy, S3 File, SQL tables, data frames / RDD, Parquet files.</li>
</ul>

<h2 id="section-2-analysis-based-on-squared-error">Section 2: Analysis based on squared error:</h2>
<ul>
  <li>Built-in PCA: https://github.com/apache/spark/blob/master/examples/src/main/python/ml/pca_example.py</li>
  <li>Built-in Regression
    <ul>
      <li>Guide: http://spark.apache.org/docs/latest/mllib-linear-methods.html#regression</li>
      <li>Python API: http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#module-pyspark.mllib.regression</li>
      <li>Example Code: https://github.com/apache/spark/blob/master/examples/src/main/python/ml/linear_regression_with_elastic_net.py</li>
    </ul>
  </li>
  <li>PCA with missing values</li>
  <li>Mahalanobis Distance</li>
  <li>K-means</li>
  <li>Compressed representation and reconstruction</li>
</ul>

<h2 id="section-3-classification">Section 3: Classification:</h2>
<ul>
  <li>Logistic regression
    <ul>
      <li>https://github.com/apache/spark/blob/master/examples/src/main/python/ml/logistic_regression_with_elastic_net.py</li>
    </ul>
  </li>
  <li>Tree-based regression
    <ul>
      <li>https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/decision_tree_regression_example.py</li>
    </ul>
  </li>
  <li>Ensamble methods for classification
    <ul>
      <li>Random forests: https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/random_forest_classification_example.py</li>
      <li>gradient boosted trees: https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/gradient_boosting_classification_example.py</li>
    </ul>
  </li>
</ul>

<h2 id="section-4-performance-tuning-measuring-and-tuning-spark-applications">Section 4: Performance tuning: measuring and tuning spark applications</h2>
<ul>
  <li>Configuration: http://spark.apache.org/docs/latest/configuration.html</li>
  <li>Monitoring: http://spark.apache.org/docs/latest/monitoring.html</li>
  <li>Tuning: http://spark.apache.org/docs/latest/tuning.html</li>
</ul>

<h2 id="section-5-spark-streaming-and-stochastic-gradient-descent">Section 5: Spark Streaming and stochastic gradient descent</h2>
<ul>
  <li>Streaming: http://spark.apache.org/docs/latest/configuration.html#spark-streaming</li>
  <li>SGD: http://spark.apache.org/docs/latest/mllib-optimization.html#stochastic-gradient-descent-sgd</li>
</ul>

## IPython Notebooks for Assignments (From Newest to Oldest) 

  <li>[Hypothesis testing](https://github.com/mGalarnyk/DSE210_Probability_Statistics_Python/blob/master/10_Hypothesis_Testing.ipynb) (1, 2, 6, 7, 8, 9, 10) </li>
  <li>[Sampling](https://github.com/mGalarnyk/DSE210_Probability_Statistics_Python/blob/master/9_Sampling.ipynb) (1, 3, 5, 8, 9, 10, 11) </li>
  <li>[Matrix factorization](https://github.com/mGalarnyk/DSE210_Probability_Statistics_Python/blob/master/8_Matrix_Factorization.ipynb) # 1,2,3,4 (PCA Projection, python),5 (PCA Projection, python) </li>
 <li>[Clustering](https://github.com/mGalarnyk/DSE210_Probability_Statistics_Python/blob/master/7_Clustering.ipynb)</li>
 <li>[Generative models 2](https://github.com/mGalarnyk/DSE210_Probability_Statistics_Python/blob/master/6_Generative_Models_number_9-FINAL.ipynb) Gaussian Classifier </li>
 <li>[Generative models 1](https://github.com/mGalarnyk/DSE210_Probability_Statistics_Python/blob/master/5_Generative_Models_I_Class_Generators.ipynb) (object oriented, next up is pandas and sql)</li>
  <li>[Random variable, expectation, and variance](https://github.com/mGalarnyk/DSE210_Probability_Statistics_Python/blob/master/4_RandomVariable_Expectation_Variance.ipynb) (1,2,3,6,7a,7c,8,12)</li>
  <li>[Multiple events, conditioning, and independence](https://github.com/mGalarnyk/DSE210_Probability_Statistics_Python/blob/master/3_Multiple_events_%20conditioning_and_independence.ipynb) (1,2,3,5,6,10,15a,16)</li>
  <li>[Probability spaces](https://github.com/mGalarnyk/DSE210_Probability_Statistics_Python/blob/master/2_Probability_spaces.ipynb) (1a,1b,1e,2,3,4a,5,6,7,14,16)</li>
  <li>[Sets and counting](https://github.com/mGalarnyk/DSE210_Probability_Statistics_Python/blob/master/1_Sets_and_Counting_mGalarnyk.ipynb) (1,2,3,4,5,6)</li>

## Assignments (From Newest to Oldest) 

  <li>[Homework 2](https://github.com/mGalarnyk/DSE230_Data_Analysis_Using_Hadoop_and_Spark_UCSD/blob/master/Homeworks/HW-2.ipynb)</li>
  <li>[Homework 1: Spark Moby Dick N Grams](https://github.com/mGalarnyk/DSE230_Data_Analysis_Using_Hadoop_and_Spark_UCSD/blob/master/Submissions/HW-1_MichaelGalarnyk.py) </li>
  
## Notes 
<li>[Timing for Regex vs string.translate and string.replace](https://github.com/mGalarnyk/DSE230_Data_Analysis_Using_Hadoop_and_Spark_UCSD/blob/master/Timing_Regex_Translate_Replace_Join.ipynb) </li>
