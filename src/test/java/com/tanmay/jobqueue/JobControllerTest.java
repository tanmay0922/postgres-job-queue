package com.tanmay.jobqueue;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@SpringBootTest(properties = "jobqueue.worker-enabled=false")
class JobControllerTest extends PostgresTest {

    @Autowired
    WebApplicationContext context;

    MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void enqueueReturnsCreatedJobWithPayloadAsJson() throws Exception {
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content("""
                        {"type": "demo.sleep", "payload": {"millis": 50}, "priority": 5}
                        """))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/jobs/1"))
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.priority").value(5))
                .andExpect(jsonPath("$.payload.millis").value(50));

        mvc.perform(get("/api/jobs/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.type").value("demo.sleep"));
    }

    @Test
    void sameIdempotencyKeyReturnsTheOriginalJob() throws Exception {
        String body = """
                {"type": "email.send", "payload": {"to": "a@b.c"}, "idempotencyKey": "signup-7"}
                """;
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated());
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1));

        mvc.perform(get("/api/jobs/stats"))
                .andExpect(jsonPath("$.QUEUED").value(1));
    }

    @Test
    void rejectsInvalidRequests() throws Exception {
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content("""
                        {"type": "", "maxAttempts": 0}
                        """))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/jobs").param("limit", "0"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void unknownJobIs404AndRetryingALiveJobIs409() throws Exception {
        mvc.perform(get("/api/jobs/999"))
                .andExpect(status().isNotFound());

        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content("""
                        {"type": "demo.sleep"}
                        """))
                .andExpect(status().isCreated());
        mvc.perform(post("/api/jobs/1/retry"))
                .andExpect(status().isConflict());
    }
}
