package com.batch.config;



import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.context.annotation.Configuration;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;
import com.batch.model.User;

@Configuration
@EnableBatchProcessing
public class BatchConfig
{
   @Autowired
   private DataSource dataSource;
	
   //Create Job
   //we can not create direct job so we'll use JobBuilderFactory
   @Autowired 
   private JobBuilderFactory jobBuilderFactory;
   
   //Create Step
   @Autowired 
   private StepBuilderFactory stepBuilderFactory;
   
   
   //Create Reader
   @Bean
   public FlatFileItemReader<User> reader()
   {
	 FlatFileItemReader<User> fileReader=new FlatFileItemReader<>();
	 
	 fileReader.setResource(new ClassPathResource("Records.csv"));
	 
	 //which line we'll read then how we will map that line
	 //we'll make getLineMapper() method
	 
	 fileReader.setLineMapper(getLineMapper());
	 fileReader.setLinesToSkip(1);
	 
	 
	 
	 return fileReader;
   }


//this method will return lineMapper   
public LineMapper<User> getLineMapper()
{
 //we will use (defaultLineMapper class) SubClass of LineMapper
	
  DefaultLineMapper<User> lineMapper=new DefaultLineMapper();	
  
 
  DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer();
  
  lineTokenizer.setNames(new String[] {"id","name","email","password"});
  lineTokenizer.setIncludedFields(new int[] {0,1,2,3});
  
  //set Mapping
  BeanWrapperFieldSetMapper<User> fieldSetMapper=new BeanWrapperFieldSetMapper<>(); 
  fieldSetMapper.setTargetType(User.class);
  
  lineMapper.setLineTokenizer(lineTokenizer);
  lineMapper.setFieldSetMapper(fieldSetMapper);
  
	return lineMapper;
}
   
   


//Create Processor
    @Bean
    public UserItemProcessor processor()
    {
	    return new UserItemProcessor();
     }
  
    
    
    

//create Writer
    //here we have two option to write data in myysql
    //1 Jpa
    //2 JdbcBatchWriter
    @Bean
    public JdbcBatchItemWriter<User> writer()
    {
      JdbcBatchItemWriter<User> writer=new JdbcBatchItemWriter<>();
      
      writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<User>());
     //here class ki property automatically set hojayegi
      
      writer.setSql("insert into user(id,name,email,password) values(:id,:name,:email,:password)");
      writer.setDataSource(dataSource);
      return writer;
    }
    
    
    
    
    
    
   // Create Job
    @Bean
   public Job importUserJob() 
   {
	   return this.jobBuilderFactory.get("USER-IMPORT-JOB")
			   .incrementer(new RunIdIncrementer())
			   .flow(step1())
			   .end()
			   .build();
   }


    
    
    
    @Bean   
    public Step step1() 
    {
	 return this.stepBuilderFactory.get("step1")
	                          .<User,User>chunk(5)
	                          .reader(reader())
	                          .processor(processor())
	                          .writer(writer())
	                          .build();
	  
}

}









