**Steps to run this project with Apache Flink:**

* Package the jar and copy into the `jars` directory.
* `docker-compose up`
* Produce data on the `profanity_words` topic with `Gun` in the values
* Consume the data on the `output-topic`