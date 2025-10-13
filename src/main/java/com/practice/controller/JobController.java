package com.practice.controller;

import java.time.Instant;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/jobs")
public class JobController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job importJob;

    @PostMapping("/import")
    public String runJob(@RequestParam String filePath) {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addString("filePath", filePath)
                    .addString("time", Instant.now().toString()) // ensure uniqueness
                    .toJobParameters();

            jobLauncher.run(importJob, params);
            return "Batch job started successfully for file: " + filePath;
        } catch (Exception e) {
            return "Failed to start job: " + e.getMessage();
        }
    }
}

