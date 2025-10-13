package com.practice.config;


import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.practice.entity.Person;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    // Step-scoped reader to read CSV dynamically
    @Bean
    @StepScope
    public FlatFileItemReader<Person> reader(@Value("#{jobParameters['filePath']}") String filePath) {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new FileSystemResource(filePath))
                .linesToSkip(1) // skip header
                .delimited()
                .names("id","firstName","lastName","email")
                .targetType(Person.class)
                .build();
    }

    // JdbcBatchItemWriter for high-performance batch inserts
    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO person (id, first_name, last_name, email) " +
                      "VALUES (:id, :firstName, :lastName, :email)");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.afterPropertiesSet();
        return writer;
    }

    // Step using StepBuilder and chunk processing
    @Bean
    public Step importStep(JobRepository jobRepository,
                           PlatformTransactionManager transactionManager,
                           FlatFileItemReader<Person> reader,
                           JdbcBatchItemWriter<Person> writer) {

        return new StepBuilder("importStep", jobRepository)
                .<Person, Person>chunk(10000, transactionManager) // large chunk for performance
                .reader(reader)
                .writer(writer)
                .taskExecutor(taskExecutor()) // multi-threaded
                .build();
    }

    // Job using JobBuilder
    @Bean
    public Job importJob(JobRepository jobRepository, Step importStep) {
        return new JobBuilder("importJob", jobRepository)
                .start(importStep)
                .build();
    }

    // TaskExecutor for multi-threading
    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor("batch-thread-");
        taskExecutor.setConcurrencyLimit(10); // number of threads
        return taskExecutor;
    }
}
