### How to test the container service

This service has no test suite,
To test it we run the tests for the node service. If these tests fail, not due to an error in the node service, we can find the container service error in the container logs, or in google cloud logging.

To modify and update the container service we run `make image_nogpu` or `make image_gpu` to re-bake the code into a new container which will be started by the node service next time we run the tests.
These containers build quickly because they only add application code to a separate base container that already exists and does not need to be updated.

1. In the node service run `make test`
2. Observe the container service logs in docker desktop
3. Modify and bake new code into a new container buy running `make image_nogpu` or `make image_gpu`
