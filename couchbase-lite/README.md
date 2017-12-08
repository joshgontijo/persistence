### Local store using Couchbase lite

```java
 public class Main {
 
    public static void main(String[] args) {
        //Saves to ~/.cblite
        CouchbaseStore<User> users = CouchbaseStore.of("users", User.class);
        users.insert("user-1", new User("John"));    
    }
}

```