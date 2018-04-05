package com.egs.jobs;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import vn.vtvcab.recomendation.entities.RequestRecommendationPersonalInCategory;

public class CountEvent implements Job<Long> {
    @Override
    public Long call(JobContext jobContext) throws Exception {
        SparkSession sparkSession = jobContext.sparkSession();
        Dataset<RequestRecommendationPersonalInCategory> dataset =

                sparkSession.read()
                            .format("org.apache.spark.sql.cassandra")
                            .option("keyspace", "recommendation")
                            .option("table", "category_recommendation")
                            .option("spark.cassandra.connection.host", "10.104.21.52,10.104.21.53,10.104.21.54")
                            .load()
                            .withColumnRenamed("account_id", "accountNumber")
                            .withColumnRenamed("category", "categoryName")
                            .as(Encoders.bean(RequestRecommendationPersonalInCategory.class));

        dataset.show();
        dataset.printSchema();
        dataset.explain();

        return null;
    }
}
