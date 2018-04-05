package com.egs;

import com.egs.jobs.CountEvent;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

public class JobSubmiter {
    public static void main(String[] args) {

        //        if (args.length != 3) {
        //            System.err.println("Fail to summit, usage: 1: livyUrl, 2: jarPath, 3: samples");
        //            System.exit(-1);
        //        }
        LivyClient client = null;
        try {

            client = new LivyClientBuilder()
                    .setURI(new URI("http://vtvcab-bigdata-db1:8998"))
                    .build();

            File jarFile = Paths.get("target", "livy-evaluation-1.0-SNAPSHOT-jar-with-dependencies.jar")
                                .toFile();
            System.out.printf("Uploading %s to the Spark context...\n", jarFile.toString());
            client.uploadJar(jarFile)
                  .get();

            int sample = 2000000;
            System.out.printf("Running CountEvent with %d samples...\n", sample);
            double pi = client.submit(new CountEvent())
                              .get();


            System.out.println("Pi is roughly: " + pi);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.stop(true);
            }
        }
    }
}
