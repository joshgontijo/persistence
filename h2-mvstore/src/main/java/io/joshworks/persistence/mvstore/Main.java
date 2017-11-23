package io.joshworks.persistence.mvstore;

import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        Map<String, List<User>> test = LocalStore.listOf("test", String.class, User.class);
//        test.put("yolo", Arrays.asList(User.of("abc", 123)));
        System.out.println(test.get("yolo"));


//        Map<String, User> test = LocalStore.of("test", String.class, User.class);
//        test.put("yolo", User.of("abc", 123));
//        System.out.println(test.get("yolo"));

        StoreManager.getStore().commit();

    }

    public static class User {
        public final  String name;
        public final int age;

        private User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public static User of(String name, int age) {
            return new User(name, age);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("User{");
            sb.append("name='").append(name).append('\'');
            sb.append(", age=").append(age);
            sb.append('}');
            return sb.toString();
        }
    }

}
