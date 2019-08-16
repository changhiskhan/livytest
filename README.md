# livytest

1. Follow Livy getting started guide https://livy.apache.org/get-started/
2. Save livy.conf (see guide above for detailed instructions) with some options:
```
livy.server.port = 8900
livy.spark.master = local
livy.spark.deploy-mode = client
livy.file.local-dir-whitelist=/<expanduser>/.livy-sessions/
```
3. Run Livy server
4. Setup local spark jars
5. Clone this repo
6. sbt package
7. sbt run

See `main` method comments for sample output
