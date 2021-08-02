"""AzureBatchManager: Performing containerized ETL jobs as Jobs/Tasks in VM Pool"""
from __future__ import print_function
import datetime
import io
import sys
import time


import azure.batch._batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels

from etl.config.general import (
    _BATCH_ACCOUNT_NAME,
    _BATCH_ACCOUNT_URL,
    _JOB_ID,
    _REGISTRY_SERVER,
    _REGISTRY_USER_NAME,
    _DOCKER_IMAGE_ABBREV,
    _DOCKER_IMAGE,
    _POOL_ID,
    _POOL_VM_SIZE,
    _POOL_NODE_COUNT,
    _STANDARD_OUT_FILE_NAME,
    _STANDARD_ERR_FILE_NAME, 
    _TASK_ID
)
from etl.config.secrets import (
    _BATCH_ACCOUNT_KEY,
    _REGISTRY_PASSWORD
)



class AzureBatchManager:  # pylint: disable=too-few-public-methods
    """Perform ETL operations as containerized workloads"""

    def __init__(self):
        super().__init__()

    def print_start_time(self):
        start_time = datetime.datetime.now().replace(microsecond=0)
        print("Sample start: {}".format(start_time))
        print()
        return start_time

    def print_end_time(self, start_time):
        # Print out some timing info
        end_time = datetime.datetime.now().replace(microsecond=0)
        print()
        print("Sample end: {}".format(end_time))
        print("Elapsed time: {}".format(end_time - start_time))

    def get_shared_key_credentials(self):
        # Create a Batch service client. We'll now be interacting with the Batch
        # service in addition to Storage
        credentials = batch_auth.SharedKeyCredentials(
            _BATCH_ACCOUNT_NAME, _BATCH_ACCOUNT_KEY
        )
        return credentials

    def get_batch_service_client(self, credentials):

        batch_client = batch.BatchServiceClient(
            credentials, batch_url=_BATCH_ACCOUNT_URL
        )
        return batch_client

    def print_batch_exception(self, batch_exception):
        """
        Prints the contents of the specified Batch exception.

        :param batch_exception:
        """
        print("-------------------------------------------")
        print("Exception encountered:")
        if (
            batch_exception.error
            and batch_exception.error.message
            and batch_exception.error.message.value
        ):
            print(batch_exception.error.message.value)
            if batch_exception.error.values:
                print()
                for mesg in batch_exception.error.values:
                    print("{}:\t{}".format(mesg.key, mesg.value))
        print("-------------------------------------------")

    def create_pool(self, batch_service_client, pool_id):
        """
        Creates a pool of compute nodes with the specified OS settings.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str pool_id: An ID for the new pool.
        :param str publisher: Marketplace image publisher
        :param str offer: Marketplace image offer
        :param str sku: Marketplace image sku
        """
        print("Creating pool [{}]...".format(pool_id))

        # Create a new pool of Linux compute nodes using an Azure Virtual Machines
        # Marketplace image. For more information about creating pools of Linux
        # nodes, see:
        # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
        """
        windows
        batch.node.windows
        amd64
        microsoftwindowsserver
        windowsserver
        2019-datacenter-with-containers
        """
        # Specify a container registry
        # We got the credentials from config.py
        containerRegistry = batchmodels.ContainerRegistry(
            user_name=_REGISTRY_USER_NAME,
            password=_REGISTRY_PASSWORD,
            registry_server=_REGISTRY_SERVER,
        )

        container_conf = batchmodels.ContainerConfiguration(
            container_image_names=[_DOCKER_IMAGE_ABBREV],
            container_registries=[containerRegistry],
        )

        new_pool = batch.models.PoolAddParameter(
            id=pool_id,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=batchmodels.ImageReference(
                    publisher="microsoftwindowsserver",
                    offer="windowsserver",
                    sku="2019-datacenter-with-containers",
                    version="latest",
                ),
                container_configuration=container_conf,
                node_agent_sku_id="batch.node.windows amd64",
            ),
            vm_size=_POOL_VM_SIZE,
            target_dedicated_nodes=_POOL_NODE_COUNT,
        )
        # Test Pool Exists
        response = batch_service_client.pool.exists(pool_id)
        if not response:
            batch_service_client.pool.add(new_pool)

    def create_job(self, batch_service_client, job_id, pool_id):
        """
        Creates a job with the specified ID, associated with the specified pool.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str job_id: The ID for the job.
        :param str pool_id: The ID for the pool.
        """
        print("Creating job [{}]...".format(job_id))

        job = batch.models.JobAddParameter(
            id=job_id, pool_info=batch.models.PoolInformation(pool_id=pool_id)
        )

        batch_service_client.job.add(job)

    def add_tasks(self, batch_service_client, job_id, data_file, mapping_file):
        """
        Adds a task for each input file in the collection to the specified job.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str job_id: The ID of the job to which to add the tasks.
        :param list input_files: A collection of input files. One task will be
         created for each input file.
        :param output_container_sas_token: A SAS token granting write access to
        the specified Azure Blob storage container.
        """

        print("Adding {} tasks to job #[{}]...".format("Python ETL Job", job_id))

        # This is the user who run the command inside the container.
        # An unprivileged one
        user = batchmodels.AutoUserSpecification(
            scope=batchmodels.AutoUserScope.task,
            elevation_level=batchmodels.ElevationLevel.admin,
        )

        # This is the docker image we want to run
        task_container_settings = batchmodels.TaskContainerSettings(
            container_run_options="--workdir=c:\\usr\\src\\app --rm",
            image_name=_DOCKER_IMAGE,
        )

        task = batchmodels.TaskAddParameter(
            id=_TASK_ID,
            command_line="python  etl\src\etl_manager.py {} {}".format(data_file, mapping_file),
            container_settings=task_container_settings,
            user_identity=batchmodels.UserIdentity(auto_user=user),
        )

        batch_service_client.task.add(job_id, task)

    def wait_for_tasks_to_complete(self, batch_service_client, job_id, timeout):
        """
        Returns when all tasks in the specified job reach the Completed state.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str job_id: The id of the job whose tasks should be to monitored.
        :param timedelta timeout: The duration to wait for task completion. If all
        tasks in the specified job do not reach Completed state within this time
        period, an exception will be raised.
        """
        timeout_expiration = datetime.datetime.now() + timeout

        print(
            "Monitoring all tasks for 'Completed' state, timeout in {}...".format(
                timeout
            ),
            end="",
        )

        while datetime.datetime.now() < timeout_expiration:
            print(".", end="")
            sys.stdout.flush()
            tasks = batch_service_client.task.list(job_id)

            incomplete_tasks = [
                task for task in tasks if task.state != batchmodels.TaskState.completed
            ]
            if not incomplete_tasks:
                print()
                return True
            else:
                time.sleep(1)

        print()
        raise RuntimeError(
            "ERROR: Tasks did not reach 'Completed' state within "
            "timeout period of " + str(timeout)
        )

    def print_task_output(self, batch_service_client, job_id, encoding=None):
        """Prints the stdout.txt file for each task in the job.

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str job_id: The id of the job with task output files to print.
        """

        print("Printing task output...")

        tasks = batch_service_client.task.list(job_id)

        for task in tasks:

            node_id = batch_service_client.task.get(job_id, task.id).node_info.node_id
            print("Task: {}".format(task.id))
            print("Node: {}".format(node_id))

            stream = batch_service_client.file.get_from_task(
                job_id, task.id, _STANDARD_OUT_FILE_NAME
            )

            file_text = self._read_stream_as_string(stream, encoding)
            print("Standard output:")
            print(file_text)

            stream = batch_service_client.file.get_from_task(
                job_id, task.id, _STANDARD_ERR_FILE_NAME
            )

            file_text = self._read_stream_as_string(stream, encoding)
            print("Standard error:")
            print(file_text)

    def _read_stream_as_string(self, stream, encoding):
        """Read stream as string

        :param stream: input stream generator
        :param str encoding: The encoding of the file. The default is utf-8.
        :return: The file content.
        :rtype: str
        """
        output = io.BytesIO()
        try:
            for data in stream:
                output.write(data)
            if encoding is None:
                encoding = "utf-8"
            return output.getvalue().decode(encoding)
        finally:
            output.close()
        raise RuntimeError("could not write data to stream or decode bytes")

    def run_job(self, data_file, mapping_file):

        try:
            start_time = self.print_start_time()

            credentials = self.get_shared_key_credentials()

            batch_client = self.get_batch_service_client(credentials)

            # Create the pool that will contain the compute nodes that will execute the tasks.
            self.create_pool(batch_client, _POOL_ID)

            # Create the job that will run the tasks.
            self.create_job(batch_client, _JOB_ID, _POOL_ID)

            # Add the tasks to the job.
            self.add_tasks(batch_client, _JOB_ID, data_file, mapping_file)

            # Pause execution until tasks reach Completed state.
            self.wait_for_tasks_to_complete(
                batch_client, _JOB_ID, datetime.timedelta(minutes=30)
            )

            print(
                "  Success! All tasks reached the 'Completed' state within the "
                "specified timeout period."
            )

            # Print the stdout.txt and stderr.txt files for each task to the console
            self.print_task_output(batch_client, _JOB_ID)

            self.print_end_time(start_time)

        except batchmodels.BatchErrorException as err:
            self.print_batch_exception(err)
            raise

if __name__ == "__main__":
    azure_batch_manager = AzureBatchManager()
    azure_batch_manager.run_job(sys.argv[1], sys.argv[2])
