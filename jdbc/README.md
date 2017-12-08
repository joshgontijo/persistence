### Example Properties ###
```properties
dataSourceClassName=org.h2.jdbcx.JdbcDataSource
dataSource.url=jdbc:h2:mem:test;DB_CLOSE_DELAY=-1
dataSource.user=sa
dataSource.password=sa
```

### Configuration ###
```java
     Jdbc.init(); // read from jdbc.properties            
     Jdbc.init(Properties properties);
     Jdbc.init(HikariDataSource ds);
```

### Streaming ###
```java
     try(Stream<Row> stream = Jdbc.stream("SELECT * FROM PUBLIC.USERS")) {
          stream.foreach(System.out::println);
     }
```

### Async blocking query ###
```java
     Jdbc.asyncQuery("SELECT * FROM PUBLIC.USERS", rows -> {
         System.out.println("User count: " + rows.size());
     });

```