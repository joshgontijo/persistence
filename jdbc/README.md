### Example Properties ###
```properties
jdbc.hikari.dataSourceClassName=org.h2.jdbcx.JdbcDataSource
jdbc.hikari.dataSource.url=jdbc:h2:mem:test;DB_CLOSE_DELAY=-1
jdbc.hikari.dataSource.user=sa
jdbc.hikari.dataSource.password=sa
jdbc.script.file=initial-schema.sql
jdbc.script.autocommit=false
jdbc.script.failonerror=true
```


### Sample usage ###
```java
    public class App {
    
        public static void main(final String[] args) {
           register(new JdbcExtension());
           start();
        }
    }

```