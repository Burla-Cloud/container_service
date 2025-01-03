### The repository for this service has been moved into the [Burla](https://github.com/Burla-Cloud/burla) GitHub repo.

It can be found in the `worker_service` folder in the top-level directory.  
This was done only to reduce cycle time. Making one github release is faster and simpler than making 4.

#### Container Service

This is a component of the open-source cluster compute software [Burla](https://github.com/Burla-Cloud/burla).

The "container service" is a flask webservice that runs inside each Docker container in the cluster, including custom containers submitted by users.
This service is the lowest abstraction layer in the Burla stack, it's the service that actually executes user-submitted functions, while streaming stdout/errors back the database where they are picked by by the [main_service](https://github.com/Burla-Cloud/main_service) and sent to the client.
