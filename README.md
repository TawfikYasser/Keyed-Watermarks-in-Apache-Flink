# Keyed Watermarks: A Partition-aware Watermark Generation in Apache Flink
### Keyed Watermarks in Apache Flink Cluster Deployment using Docker

![Vanilla Vs. Keyed WM](https://github.com/TawfikYasser/Keyed-Watermarks-in-Apache-Flink/blob/main/vanillavskeyedwm.png)

The repository is structured as follows:
* Explanation for the idea of Keyed Watermarks.
* How to set up and run the cluster to be able to run Apache Flink with Keyed Watermarks.
* The repo. contains the jars, source java classes of Keyed Watermarks, pipeline to test with, docker file used to build the cluster, and instructions on how to use everything.

#### What is `Keyed Watermarks` 
Big Data Stream processing engines, exemplified by tools like Apache Flink, employ windowing techniques to manage unbounded streams of events. The aggregation of relevant data within Windows holds utmost importance for event-time windowing due to its impact on result accuracy. A pivotal role in this process is attributed to watermarks, unique timestamps signifying event progression in time. Nonetheless, the existing watermark generation method within Apache Flink, operating at the input stream level, exhibits a bias towards faster sub-streams, causing the omission of events from slower counterparts. Through our analysis, we determined that Apache Flink's standard watermark generation approach results in an approximate $33\%$ data loss when $50\%$ of median-proximate keys experience delays. Furthermore, this loss exceeds $37\%$ in cases where $50\%$ of randomly selected keys encounter delays. We introduce a pioneering approach termed keyed watermarks aimed at addressing data loss concerns and enhancing data processing precision to a minimum of $99\%$ in the majority of scenarios. Our strategy facilitates distinct progress monitoring through the creation of individualized watermarks for each logical sub-stream (key).

![Experimental Setup](https://github.com/TawfikYasser/Keyed-Watermarks-in-Apache-Flink/blob/main/ExperimentalSetup.png)

#### Set up an Apache Flink Cluster using [`Docker`](https://github.com/TawfikYasser/kw-flink-cluster-docker/blob/main/Dockerfile)
* Clone this repo. using the following command: `git clone https://github.com/TawfikYasser/kw-flink-cluster-docker.git`.
* Download the `build-target` folder from this [`link`](https://drive.google.com/drive/folders/1_gEHB0FxrvtpiAGlCqfd4GLXfACmn2As?usp=sharing) and put it in the same directory of the repo.
* Clone this repo. and in the same directory run the following command: `docker build -t <put-your-docker-image-name-here> .`, a docker image will be created.
* Then we need to create 3 docker containers, one for the `JobManager`, and two for the `TaskManagers`.
   * To create the `JobManager` container run the following command: `docker run -it --name <put-your-docker-container-name-here> -p 8081:8081 --network bridge <put-your-docker-image-name-here>:latest`, we're exposing the 8081 port in order to be able to access the Apache Flink Web UI from outside the containers, also we're attaching the container to the `bridge` network.
   * To create the `TaskManagers` run the following two commands: `docker run -it --name  <put-your-docker-container-name-here> --network bridge <put-your-docker-image-name-here>:latest`, `docker run -it --name  <put-your-docker-container-name-here> --network bridge <put-your-docker-image-name-here>:latest`.
* Now, you have to configure the `masters`, `workers`, and `flink-config.yml` files on each container as follows:
   * [`masters file`](https://github.com/TawfikYasser/kw-flink-cluster-docker/blob/main/configurations/masters.txt)
   * [`workers file`](https://github.com/TawfikYasser/kw-flink-cluster-docker/blob/main/configurations/workers.txt)
   * [`flink-config.yml file`](https://github.com/TawfikYasser/kw-flink-cluster-docker/blob/main/configurations/flink-config.yml)
* Start the containers, and start the `ssh` service on the containers of taskManagers using: `service ssh start`.
#### Ready!
* Now, you're ready to start the cluster, go to `/home/flink/bin/` inside the container containing the jobManager and run the cluster using: `./start-cluster.sh`.
* Run a Flink job inside `/home/flink/bin/` using the following command: `./flink run <pipeline-jar-path>`. (-p <parallelism> optional if you want to override the default parallelism in `flink-config.yml`)
* Open the Web UI of Apache Flink using: `http://localhost:8081`. (If you're running the containers on a VM use the VM's External IP, otherwise use your local machine's IP)

#### Flink Pipeline w/ `Keyed Watermarks`
* In the following [`java`](https://github.com/TawfikYasser/kw-flink-cluster-docker/blob/main/pipeline/kw.java) file, use it as a starting point.

#### Citation

```
@article{keyed-watermarks,
  title={},
  author={},
  journal={},
  year={}
}
```

---

IMPORTANT: [`Code Base of Keyed Watermarks`](https://github.com/TawfikYasser/kw-flink-cluster-docker/tree/main/keyed-watermarks-code-base) & [`Ready to Run Flink Cluster`](https://drive.google.com/drive/folders/1_gEHB0FxrvtpiAGlCqfd4GLXfACmn2As)

---
Tawfik Yasser Contact Info.: [`LinkedIn`](https://www.linkedin.com/in/tawfikyasser/), [`Email`](mailto:tyasser@nu.edu.eg), and [`Resume`](https://drive.google.com/file/d/1f_JfUcS0jrBsMIeddgL883GeizyYkBcr/view?usp=sharing).
