# hellospark
### Examples of Spark features in **Scala** to test it out.
##### We will need to add below VM options when running it to bypass the serialization issue.
```bash
--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED
```
##### For streaming, we need to run below in terminal first.
```bash
nc -lk 9999
```