## Persistent Map with H2 MVStore and Kryo ###

### Usage ###
```java
 public class Main {
 
     public static void main(String[] args) {
         //Saves under ~/.mvstore/db.dat
         Map<String, User> store = LocalStore.listOf("users", String.class, User.class);
         
     }
}
```