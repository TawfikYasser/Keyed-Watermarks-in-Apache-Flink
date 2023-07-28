FROM ubuntu

# Step 1: Install required packages and SSH
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    vim \
    ssh \
    && rm -rf /var/lib/apt/lists/*

# Step 2: Generate SSH keys
RUN mkdir -p /root/.ssh && ssh-keygen -t rsa -f /root/.ssh/id_rsa -q -N ""

# Step 3: Copy the public SSH key to authorized_keys
RUN cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys

# Step 4: Install Apache Flink [From the build-target (Attached in the repo.)]
RUN mkdir -p /home/flink/
COPY /build-target /home/flink/

CMD ["bash"]
