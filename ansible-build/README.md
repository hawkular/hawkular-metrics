
# Hawkular Docker Container Ansible Playbook 

This playbook generates a Hawkular Docker Image

## Requirements

- ``Git``
- ``Maven``
- ``Java``
- ``Docker and Docker-py``
- ``Ansible and Ansible-Playbook``


## Usage

* Clone the hawkular-metrics on a specific branch/tag/commit (If the desired commit/branch/tag doesn't have the `ansible-build` folder, it should be copied from another commit/branch/tag).

* Run the Standard building command:

`mvn-clean-install -DskipTests` 

> if you want to make any change on the code, you can do it then run the `mvn command`


* Once the building process finalized with success, it is necessary to export the following environment variables:

 ``export HAWKULAR_METRICS_TAG='my-tag'``
 ``export CASSANDRA_VERSION='3.0.14'``


* If you need to deploy the image on Docker Hub, you will need to change the hawkular_metrics_image_name on ``roles/create_image/defaults/main.yml``, eg:

``hawkular_metrics_image_name: "usename_on_docker_hub/hawkular-metrics"`` 


* Finally, Run ``ansible-playbook -i hosts playbooks/hawkular.yml  -vvv`` to create a hawkular metrics docker container (Make sure that Docker is running and that you have all the requirements)


* At the end of playbook running, a file called ``docker-compose.yml`` will be generated on ``ansible-build`` folder.


* You may start Hawkular Metrics and Cassandra running `docker-compose up` inside ``ansible-build`` folder


