# chaining-job
Simple chaining job builder for Apache Hadoop

Repository management:
```xml
<repository>
    <id>chaining-job-mvn-repo</id>
    <url>https://raw.github.com/zydins/chaining-job/mvn-repo/</url>
    <snapshots>
        <enabled>true</enabled>
        <updatePolicy>always</updatePolicy>
    </snapshots>
</repository>
```

Dependency management:
```xml
<dependency>
    <groupId>ru.zudin</groupId>
    <artifactId>chaining-job</artifactId>
    <version>0.1.0</version>
</dependency>
```
Usage:
```java
ChainingJob job = ChainingJob.Builder.instance()
                .name("triclustering")
                .tempDir(temp)
                .mapper(TupleReadMapper.class, ImmutableMap.of("delimeter", ";"))
                .reducer(TupleContextReducer.class)
                .mapper(PrepareMapper.class)
                .reducer(CollectReducer.class, ImmutableMap.of("threads", "2")
                .build();
        job.getJob(0).setNumReduceTasks(2);
        ToolRunner.run(new Configuration(), job, new String[]{"input", "output"});
```
