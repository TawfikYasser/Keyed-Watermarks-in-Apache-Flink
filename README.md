# Keyed Watermarks: A Fine-grained Watermark Generation for Apache Flink 
### Keyed Watermarks in Apache Flink Cluster Deployment using Docker

![Vanilla Vs. Keyed WM](https://github.com/TawfikYasser/Keyed-Watermarks-in-Apache-Flink/blob/main/scenario.png)

The repository is organized as follows:

* An explanation of the concept of Keyed Watermarks.
* Instructions for setting up and running a cluster to use Apache Flink with Keyed Watermarks.
* The repository includes:
    * JAR files and source Java classes for Keyed Watermarks.
    * A testing pipeline.
    * A Dockerfile for building the cluster.
    * Detailed usage instructions.

#### What is `Keyed Watermarks` 
Big Data Stream processing engines, exemplified by tools like Apache Flink, employ windowing techniques to manage unbounded streams of events. The aggregation of relevant data within Windows holds utmost importance for event-time windowing due to its impact on result accuracy. A pivotal role in this process is attributed to watermarks, unique timestamps signifying event progression in time. Nonetheless, the existing watermark generation method within Apache Flink, operating at the input stream level, exhibits a bias towards faster sub-streams, causing the omission of events from slower counterparts. Through our analysis, we determined that Apache Flink's standard watermark generation approach results in an approximate $33\%$ data loss when $50\%$ of median-proximate keys experience delays. Furthermore, this loss exceeds $37\%$ in cases where $50\%$ of randomly selected keys encounter delays. We introduce a pioneering approach termed keyed watermarks aimed at addressing data loss concerns and enhancing data processing precision to a minimum of $99\%$ in the majority of scenarios. Our strategy facilitates distinct progress monitoring through the creation of individualized watermarks for each logical sub-stream (key).

![Experimental Setup](https://github.com/TawfikYasser/Keyed-Watermarks-in-Apache-Flink/blob/main/ExperimentalSetup.png)

#### Set up an Apache Flink Cluster using [`Docker`](https://github.com/TawfikYasser/kw-flink-cluster-docker/blob/main/Dockerfile)
* Clone this repo. using the following command: `git clone https://github.com/TawfikYasser/Keyed-Watermarks-in-Apache-Flink.git`.
* Download the `flink` source code with keyed watermarks from the following [link](https://drive.google.com/drive/folders/1Tq95uxpyzlph5SN5vq4upQg4_bYm8TQb?usp=sharing).
* Now, you can build flink by `cd` to the source code you've downloaded in the previous step using the following [instructions](https://nightlies.apache.org/flink/flink-docs-master/docs/flinkdev/building/#build-flink).
* After the successful build of flink, put the `build-target` folder inside the repo. folder.
* Now, the repo. contains the following files:
  * /build-target
  * /configurations
  * /keyed-watermarks-code-base
  * /pipeline
  * Dockerfile
  * ExperimentalSetup.png
  * LICENSE
  * README.md
  * code-listings.txt
  * scenario.png
* Run the following command in the repo.: `docker build -t <put-your-docker-image-name-here> .`, a docker image will be created. We will use it next to create the containers.
* Then we need to create 3 docker containers, one for the `JobManager`, and two for the `TaskManagers`.
   * To create the `JobManager` container run the following command:
     * `docker run -it --name JM -p 8081:8081 --network bridge <put-your-docker-image-name-here>:latest`.
     * We're exposing the 8081 port in order to be able to access the Apache Flink Web UI from outside the containers.
     * Also we're attaching the container to the `bridge` network.
   * To create the `TaskManagers` run the following two commands:
     * `docker run -it --name TM1 --network bridge <put-your-docker-image-name-here>:latest`.
     * `docker run -it --name TM2 --network bridge <put-your-docker-image-name-here>:latest`.
* Now, you have to configure the `masters`, `workers`, and `flink-config.yml` files on each container as follows:
   * [`masters file`](https://github.com/TawfikYasser/kw-flink-cluster-docker/blob/main/configurations/masters.txt)
   * [`workers file`](https://github.com/TawfikYasser/kw-flink-cluster-docker/blob/main/configurations/workers.txt)
   * [`flink-config.yml file`](https://github.com/TawfikYasser/kw-flink-cluster-docker/blob/main/configurations/flink-config.yml)
* Start the containers, and start the `ssh` service on the containers of taskManagers using the following command: `service ssh start`.

#### Ready!
* Now, you're ready to start the cluster, open the `JobManager` container using: `docker exec -it JM bash`.
* Go to `/home/flink/bin/` and run the cluster using: `./start-cluster.sh`.
* To copy any file (i.e. the job jar file) into the container, use the following command outside the container: `docker cp <file-path> JM:<path-inside-the-container>`.
* Run a Flink job inside `/home/flink/bin/` using the following command: `./flink run <jar-path-inside-the-container>`.
  * (-p <parallelism> optional if you want to override the default parallelism in `flink-config.yml`)
* Open the Web UI of Apache Flink using: `http://localhost:8081`.
  * (If you're running the containers on a VM use the VM's External IP, otherwise use your local machine's IP)

#### Datasets
* All datasets used are in this [link](https://drive.google.com/drive/folders/1F3ageBfsfOXqHKrk0H0ItqkJ4WJr_lQd?usp=sharing).
* The main dataset used contains around 6M records.
* The secondary dataset used contains around 49M records.

#### Out Of Order Data Generator
* Code could be found [here](https://drive.google.com/drive/folders/1Hkza13L3HfT8U7eVvLBOLnXrxN8r6Zhr?usp=sharing).

#### Flink Pipeline w/ `Keyed Watermarks`
* The pipeline code contains the flink java code to run the experiments of Accuracy, Latency, and State Size for all datasets.
* All you need is to build the pipeline project and copy the `.jar` file into the JM container to use to run a flink job.
* In the pipelines project you need to import the following jar files:
  * flink-connector-files-1.17-SNAPSHOT.jar
  * flink-dist_2.12-1.17-SNAPSHOT.jar
  * flink-shaded-zookeeper-3-3.5.9-15.0.jar
  * log4j-api-2.20.0
  * log4j-core-2.20.0
* **Accuracy Experiments:**
  1. You've built the flink source code and got the `build-target`.
  2. Update the input and output paths in the pipeline code before building the pipeline jar.
  3. Car IDs text files are [here](https://drive.google.com/drive/folders/1-Gi7heqdcBCpnIUfpq5LzAp84Jhf0dMJ?usp=sharing).
  4. Next, build the pipeline code of the accuracy experiments, you can find it [here](https://drive.google.com/drive/folders/1E8FLGTRq88k9glyrR7bt9IsUKOzfQFPx?usp=sharing).
  5. Move the pipeline jar into the container, then run the flink job using: `./bin/flink run <jar-path-inside-the-container>`.
  6. After running the job, 8 sub-jobs (for each dataset) will generate output files.
  7. Finally, you can run the Python pipelines to generate the final results & graphs of accuracy.
* **Latency Experiments (Keyed):**
  1. Same steps as in Accuracy with some changes.
  2. For Keyed we need to uncomment some lines in the class `KeyedTimestampsAndWatermarksOperator` to allow logging the start timestamp of watermark generation.
  3. Also, we need to allow logging the end timestamp of watermark generation in the class `WindowOperator`.
  4. Finally, re build the project, replace the new build-target with the existing one inside the containers and run the latency job pipeline.
  5. `WindowOperator` class for Latency experiments could be found [here](https://drive.google.com/drive/folders/16_jsF-z_55_NvG187NLVj6KG8NwKItXm?usp=sharing).
* **Latency Experiments (Non-keyed):**
  1. Same steps as in Accuracy with some changes.
  2. For Keyed we need to uncomment some lines in the class `TimestampsAndWatermarksOperator` to allow logging the start timestamp of watermark generation.
  3. Also, we need to allow logging the end timestamp of watermark generation in the class `WindowOperator`.
  4. Finally, re build the project, replace the new build-target with the existing one inside the containers and run the latency job pipeline.
  5. `WindowOperator` class for Latency experiments could be found [here](https://drive.google.com/drive/folders/16_jsF-z_55_NvG187NLVj6KG8NwKItXm?usp=sharing).
* **State Size Experiments (Keyed):**
 1. Same steps as in Accuracy with some changes.
  3. We need to allow logging the state size in the class `WindowOperator`, replace the `WindowOperator` class with the attached one [here](https://drive.google.com/drive/folders/16_jsF-z_55_NvG187NLVj6KG8NwKItXm?usp=sharing). (Use the default `TimestampsAndWatermarksOperator` and `KeyedTimestampsAndWatermarksOperator` classes)
  4. Finally, re build the project, replace the new build-target with the existing one inside the containers and run the latency job pipeline.

#### Citation

```
@INPROCEEDINGS{10296717,
  author={Yasser, Tawfik and Arafa, Tamer and El-Helw, Mohamed and Awad, Ahmed},
  booktitle={2023 5th Novel Intelligent and Leading Emerging Sciences Conference (NILES)}, 
  title={Keyed Watermarks: A Fine-grained Tracking of Event-time in Apache Flink}, 
  year={2023},
  volume={},
  number={},
  pages={23-28},
  doi={10.1109/NILES59815.2023.10296717}}
```
---

IMPORTANT: [`Code Base of Keyed Watermarks`](https://github.com/TawfikYasser/kw-flink-cluster-docker/tree/main/keyed-watermarks-code-base) & [`Ready to Run Flink Cluster`](https://drive.google.com/drive/folders/1_gEHB0FxrvtpiAGlCqfd4GLXfACmn2As)

---
Tawfik Yasser Contact Info.: [`LinkedIn`](https://www.linkedin.com/in/tawfikyasser/), [`Email`](mailto:tawfekyassertawfek@gmail.com), and [`Resume`](https://drive.google.com/file/d/12KDxirYHipfH5bOuHgJtcBB6CmiHmqdM/view?usp=sharing).
