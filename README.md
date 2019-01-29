## Practical Course Cloud Databases - MS5: MapReduce Framework for distributed Key-Value Store 

- If the ECS cannot ssh the servers, please verify your ssh username and your path to the server jar file in the 3rd constructor of src/ecs/KVServer.java. The path may be different on different OS.

- Word-count and Inverted-Index applications are implemented to demonstrate the framework. Please use commands 'wordcount' and 'search' on the client CLI.

- The test src/testing/performance/PerfTest.java is used to test the performance of the service. However, it can also be considered/used as integration test to make sure the service is working properly. Note that we can adjust these parameters: amount of loaded data, OPS_PER_CLIENT to have the system load less data for quick test.

