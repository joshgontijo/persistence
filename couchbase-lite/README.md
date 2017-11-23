### Local store using Couchbase lite


```java

        String location = System.getProperty("user.home") + "/localstore";
        LocalStore.init(new Manager(new SnappyStoreContext(location), Manager.DEFAULT_OPTIONS), new DatabaseOptions());
    
        User user = new User(10, "asd");
        LocalStore.create("test", user.id, user);
    
        User found = LocalStore.get("test", user.id, User.class);
        found.name = "josh";
        LocalStore.update("test", user.id, found);
    
        found = LocalStore.get("test", user.id, User.class);
    
        Map<String, Object> merge = new HashMap<>();
        merge.put("age", 999);
    
        LocalStore.merge("test", user.id, merge);
    
        System.out.println(found);
        
        
        //Using Couchbase directly
        Manager manager = LocalStore.getManager();
        //or
        Database database = LocalStore.getDatabase("db-name");
        

```