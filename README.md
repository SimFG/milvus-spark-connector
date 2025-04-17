# Milvus Spark Connector

## Environment Preparation

To ensure smooth operation, it is critical to use consistent versions of the required tools. Mismatched versions may lead to compatibility issues.

1. **SDKMAN** is recommended for managing Scala and Spark environments.
2. **Java Version:** 17.0
3. **Scala Version:** 2.13.8
4. **Spark Version:** 3.5.3 (built with Scala 2.13)
5. **SBT Version:** 1.10.11

If you are using SDKMAN, you can quickly install Java, Scala, and SBT as follows:

```bash
sdk install java 17.0.14.crac-zulu
sdk install scala 2.13.8
sdk install sbt 1.10.11
```

The Spark version provided by SDKMAN only supports Scala 2.12. Therefore, you need to manually install the Spark version compatible with Scala 2.13. You can download it from the following link: [Spark Download](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3-scala2.13.tgz).

### Spark Submit Configuration

To simplify the `spark-submit` process, create a wrapper script named `spark-submit-wrapper.sh`. Replace the `SPARK_HOME` path with the actual installation directory on your machine.

```bash
#!/bin/bash

export SPARK_HOME=/xxx/spark-3.5.3-bin-hadoop3-scala2.13

if [ ! -d "$SPARK_HOME" ]; then
  echo "Error: SPARK_HOME directory does not exist: $SPARK_HOME"
  exit 1
fi

SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
if [ ! -f "$SPARK_SUBMIT" ]; then
  echo "Error: spark-submit not found at: $SPARK_SUBMIT"
  exit 1
fi

exec "$SPARK_SUBMIT" "$@"
```

After saving the script, grant it execution permissions and set up an alias for convenience. Replace the script path with the actual location on your machine.

```bash
chmod +x /xxx/spark-submit-wrapper.sh
alias spark-submit-wrapper="/xxx/spark-submit-wrapper.sh"
```

## Running the Connector

### Build and Package Milvus Spark Connector

Use the following SBT commands to compile, package, and publish the connector locally:

```bash
sbt clean compile package publishLocal
```

- **clean:** Clears previous build artifacts.
- **compile:** Compiles the source code.
- **package:** Packages the compiled code into a JAR file.
- **publishLocal:** Publishes the package to the local repository (primarily for use in connector examples).

To create a fat JAR containing all dependencies, run:

```bash
sbt assembly
```

### Build and Package Milvus Spark Connector Example

Example Project Repository: [Milvus Spark Connector Example](https://github.com/SimFG/milvus-spark-connector-example)

To compile and package the example project, use the following SBT command:

```bash
sbt clean compile package
```

### Run the Test Demo

To execute the test demo, specify the paths to the JAR files generated in the previous steps. Replace `/xxx/` with the actual paths on your machine.

```bash
spark-submit-wrapper --jars /xxx/spark-connector-assembly-0.1.0-SNAPSHOT.jar --class "example.FloatInsertDemo" /xxx/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar
```

This command runs the `FloatInsertDemo` class, which demonstrates how to insert data into Milvus using the Spark connector. Ensure that the paths to the JAR files are correct before running the command.