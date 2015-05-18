import yarn.protobuf.applicationclient_protocol_pb2 as application_client_protocol
import yarn.protobuf.yarn_service_protos_pb2 as yarn_service_protos
import yarn.protobuf.yarn_protos_pb2 as yarn_protos
import snakebite.glob as glob
from snakebite.errors import RequestError
from yarn.rpc.service import RpcService
from snakebite.errors import FileNotFoundException
from snakebite.errors import DirectoryException
from snakebite.errors import FileException
from snakebite.errors import InvalidInputException
from snakebite.errors import OutOfNNException
from snakebite.channel import DataXceiverChannel
from snakebite.config import HDFSConfig
from . import YARN_PROTOCOL_VERSION
import urlparse

import logging

log = logging.getLogger(__name__)

class Client(object):
    """
    A pure python Yarn client. Yarn clients are used for submitting jobs to a Yarn ResourceManager
    """

    def __init__(self, host, port, hadoop_version=YARN_PROTOCOL_VERSION):
        '''
        :param host: Hostname or IP address of the ResourceManager
        :type host: string
        :param port: RPC Port of the ResourceManager
        :type port: int
        :param hadoop_version: What hadoop protocol version should be used (default: 9)
        :type hadoop_version: int
        '''
        if hadoop_version < 9:
            raise Exception("Only protocol versions >= 9 supported")

        context_proto = "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB"
        self.host = host
        self.port = port
        self.service_stub_class = application_client_protocol.ApplicationClientProtocolService_Stub
        self.service = RpcService(self.service_stub_class, context_proto, self.port, self.host, hadoop_version)

        log.debug("Created client for %s:%s", host, port)

    def get_applications(self, app_states=None):
        ALL_APP_STATES = [
            yarn_protos.ACCEPTED,
            yarn_protos.NEW,
            yarn_protos.NEW_SAVING,
            yarn_protos.SUBMITTED,
            yarn_protos.RUNNING,
            yarn_protos.FINISHED,
            yarn_protos.KILLED,
            yarn_protos.FAILED
            ]
            
        req = yarn_service_protos.GetApplicationsRequestProto()
        if not app_states:
            app_states = ALL_APP_STATES
        elif type(app_states) in (tuple, list):
            pass
        else:
            app_states = [app_states]

        for app_state in app_states:
            req.application_states.append(app_state)
             
        response = self.service.getApplications(req)
        if response:
            return response.applications
        else:
            return []

    def get_application_report(self, cluster_timestamp, app_id):
        """
        :type cluster_timestamp: long
        :type app_id: int
        """
        req = yarn_service_protos.GetApplicationReportRequestProto()
        req.application_id.id = int(app_id)
        req.application_id.cluster_timestamp = cluster_timestamp
        return self.service.getApplicationReport(req)

    def submit_application(self, cluster_timestamp, app_id, priority, am_container_spec,
        resource, keep_containers_across_application_attempts, application_tags,
        application_name="N/A", unmanaged_am=False, cancel_tokens_when_complete=True,
        max_attempts=0, application_type="YARN", queue="default"):
        if not isinstance(resource, yarn_protos.ResourceProto):
            resource = yarn_protos.ResourceProto(**resource)
        if not isinstance(am_container_spec, yarn_protos.ContainerLaunchContextProto):
            am_container_spec = container_launch_context(**am_container_spec)
        if not isinstance(priority, yarn_protos.PriorityProto):
            priority = yarn_protos.PriorityProto(**priority)

        req = yarn_service_protos.SubmitApplicationRequestProto(
            application_submission_context=yarn_protos.ApplicationSubmissionContextProto(
                applicationTags=application_tags,
                keep_containers_across_application_attempts=keep_containers_across_application_attempts,
                unmanaged_am=unmanaged_am,
                application_name=application_name,
                applicationType=application_type,
                cancel_tokens_when_complete=cancel_tokens_when_complete,
                maxAppAttempts=max_attempts,
                queue=queue,
                priority=priority,
                application_id=yarn_protos.ApplicationIdProto(id=app_id, cluster_timestamp=cluster_timestamp),
                am_container_spec=am_container_spec,
                resource=resource
                )
            )
        return self.service.submitApplication(req)

    def get_new_application(self):
        req = yarn_service_protos.GetNewApplicationRequestProto()
        return self.service.getNewApplication(req)

    def get_cluster_nodes(self):
        req = yarn_service_protos.GetClusterNodesRequestProto()
        response = self.service.getClusterNodes(req)
        if response:
            return response.nodeReports
        else:
            return []

    def get_cluster_metrics(self):
        req = yarn_service_protos.GetClusterMetricsRequestProto()
        resp = self.service.getClusterMetrics(req)
        return resp

    def get_queue_info(self, queue='root'):
        req = yarn_service_protos.GetQueueInfoRequestProto()
        req.queueName = queue
        return self.service.getQueueInfo(req)

    def get_queue_user_acls(self):
        req = yarn_service_protos.GetQueueInfoRequestProto()
        return self.service.getQueueUserAcls(req)

    def get_containers(self, cluster_timestamp, app_id):
        req = yarn_service_protos.GetContainersRequestProto()
        req.application_attempt_id.application_id.id = app_id
        req.application_attempt_id.application_id.cluster_timestamp = cluster_timestamp
        return self.service.getContainers(req)

    def get_container_report(self, cluster_timestamp, app_id, container_id):
        req = yarn_service_protos.GetContainerReportRequestProto()
        req.container_id.id = container_id
        req.container_id.application_id.id = app_id
        req.container_id.application_id.cluster_timestamp
        return self.service.getContainerReport(req)

    def force_kill_application(self, cluster_timestamp, app_id):
        req = yarn_service_protos.KillApplicationRequestProto()
        req.application_id.id = app_id
        req.application_id.cluster_timestamp = cluster_timestamp
        return self.service.forceKillApplication(req)

    def move_application_across_queues(self, cluster_timestamp, app_id, target_queue):
        req = yarn_service_protos.MoveApplicationAcrossQueuesRequestProto(
            application_id=yarn_service_protos.ApplicationIdProto(
                id=app_id,
                cluster_timestamp=cluster_timestamp
                ),
            target_queue=target_queue
            )
        return self.service.moveApplicationAcrossQueue(req)

    def get_application_attempt_report(self, cluster_timestamp, app_id):
        req = yarn_service_protos.GetApplicationAttemptReportRequestProto(
            application_id=yarn_protos.ApplicationIdProto(
                id=app_id,
                cluster_timestamp=cluster_timestamp
                )
            )
        return self.service.getApplicationAttemptReport(req)

    def get_application_attempts(self, cluster_timestamp, app_id):
        req = yarn_service_protos.GetApplicationAttemptsRequestProto(
            application_id=yarn_protos.ApplicationIdProto(
                id=app_id,
                cluster_timestamp=cluster_timestamp
                )
            )
        return self.service.getApplicationAttempts(req)

    def get_delegation_token(self):
        pass

    def renew_delegation_token(self):
        pass

    def cancel_delegation_token(self):
        pass

def local_resource(url, type_, visibility, timestamp, size, pattern=None):
    url = urlparse.urlparse(url)
    url = yarn_protos.URLProto(scheme=url.scheme, file=url.path,
        host=url.hostname, port=url.port, 
        userInfo=url.username + ':' + url.password)
    resource = yarn_protos.LocalResourceProto(
        resource=url,
        visibility=yarn_protos.LocalResourceVisibilityProto.Value(visibility.upper()),
        type=yarn_protos.LocalResourceTypeProto.Value(type_.upper()),
        timestamp=timestamp,
        size=size
        )
    if pattern and type_.upper() == 'PATTERN':
        resource.pattern = pattern
    return resoruce

def container_launch_context(command, resources, tokens='', environment={}, service_data={}, application_acls=[]):
    c = yarn_protos.ContainerLaunchContextProto(
        command=command,
        localResources=[yarn_protos.StringLocalResourceMapProto(key=k, value=local_resource(**v)) for k, v in resources.iteritems()],
        toekns=tokens,
        environment=[yarn_protos.StringStringMapProto(key=k, value=v) for k, v in environment.iteritems()],
        service_data=[yarn_protos.StringBytesMapProto(key=k, value=v) for k, v in service_data.iteritems()],
        application_ACLs=[yarn_protos.ApplicationACLMapProto(acl=acl, accessType=yarn_protos.ApplicationAccessTypeProto.Value(access_type)) for access_type, acl in application_acls]
        )
    return c