package com.tanmay.jobqueue;

import java.io.IOException;

import org.springframework.boot.SpringApplication;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

/**
 * Runs the app against a throwaway embedded PostgreSQL, so you can try it
 * without installing Postgres or Docker:
 *
 * <pre>./mvnw spring-boot:test-run</pre>
 */
public class LocalJobQueueApplication {

    public static void main(String[] args) throws IOException {
        EmbeddedPostgres postgres = EmbeddedPostgres.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                postgres.close();
            } catch (IOException ignored) {
                // The process is exiting anyway.
            }
        }));

        System.setProperty("spring.datasource.url", postgres.getJdbcUrl("postgres", "postgres"));
        System.setProperty("spring.datasource.username", "postgres");
        System.setProperty("spring.datasource.password", "");
        SpringApplication.run(JobQueueApplication.class, args);
    }
}
