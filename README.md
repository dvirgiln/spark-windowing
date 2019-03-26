# spark-windowing

## Instructions usage using docker-compose
    1. sbt docker
    2. docker swarm init
    3. docker stack deploy -c docker-compose.yml spark-windowing
    4. docker service ls
    5. docker service logs -f spark-windowing_producer
    6. docker service logs -f spark-windowing_spark-consumer
    8. docker stack rm spark-windowing
    9. docker swarm leave --force
