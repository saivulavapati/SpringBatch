package com.practice.controller;

import java.io.File;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobController {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job importJob;

	@PostMapping("/import")
	public ResponseEntity<?> runJob(@RequestParam String filePath) {
		try {
			File file = new File(filePath);
			if (!file.exists() || !file.canRead()) {
				return ResponseEntity.badRequest().body("File not found or not readable: " + filePath);
			}

			JobParameters jobParameters = new JobParametersBuilder().addString("filePath", filePath)
					.addLong("timestamp", System.currentTimeMillis()).toJobParameters();

			JobExecution jobExecution = jobLauncher.run(importJob, jobParameters);

			while (jobExecution.isRunning()) {
				Thread.sleep(500);
			}

			return ResponseEntity.ok("Job finished with status: " + jobExecution.getStatus());

		} catch (FlatFileParseException e) {
			return ResponseEntity.badRequest()
					.body("CSV parse error at line " + e.getLineNumber() + ": " + e.getInput());
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e) {
			return ResponseEntity.badRequest().body("Job error: " + e.getMessage());
		} catch (Exception e) {
			return ResponseEntity.internalServerError().body("Unexpected error: " + e.getMessage());
		}
	}
}
