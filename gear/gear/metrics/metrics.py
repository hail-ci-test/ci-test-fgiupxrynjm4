from google.cloud import monitoring_v3
from collections import defaultdict
from functools import wraps
import asyncio
import datetime
from typing import Dict, Callable
from google.api_core.exceptions import GoogleAPIError

from hailtop.config import get_deploy_config


PROJECT_ID = 'hail-vdc'
CLUSTER_NAME = 'vdc'
RESOURCE_TYPE = 'k8s_container'
METRIC_BASE = 'custom.googleapis.com'
MIN_GRANULARITY_SECS = 60
metrics_uploader = None

deploy_config = get_deploy_config()


def init():
    global metrics_uploader
    resource_data = ResourceData(
        resource_type=RESOURCE_TYPE,
        cluster_name=CLUSTER_NAME,
        namespace_name=deploy_config.default_namespace(),
        location='us-central1-a',
        container_name='ci',
        pod_name='ci',
    )
    if metrics_uploader is None:
        metrics_uploader = Metrics(PROJECT_ID, resource_data)


async def update_loop():
    await metrics_uploader.run()


# In simplest terms: a GAUGE is an instantaneous measurement,
# while a CUMULATIVE (counter) is a measurement that should be
# interpreted as part of a collection of points with the same
# start_time and different end_time and are by default presented
# as rates (things per time interval)
# A 1-point counter is effectively identical to a GAUGE, save for
# the difference in interpretation of X at this time vs X per time interval
# at this time

def count(metric_name):
    def wrap(fun):
        @wraps(fun)
        async def wrapped(*args, **kwargs):
            metrics_uploader.increment(metric_name)
            return await fun(*args, **kwargs)
        return wrapped
    return wrap


def gauge(metric_name):
    def wrap(fun):
        metrics_uploader.gauge_callbacks[metric_name] = fun
        return fun
    return wrap


class Metrics:
    def __init__(self, project_id, resource_data):
        self.project_id = project_id
        self.resource_data = resource_data
        self.counters = defaultdict(monitoring_v3.Point)
        self.gauges = defaultdict(monitoring_v3.Point)
        self.gauge_callbacks: Dict[str, Callable] = dict()

        self.client = monitoring_v3.MetricServiceClient()

    async def run(self):
        while True:
            await asyncio.sleep(MIN_GRANULARITY_SECS)
            self.query_gauges()
            if len(self.counters) > 0 or len(self.gauges) > 0:
                self.push_metrics()

    def increment(self, metric_name):
        self.counters[metric_name].value.double_value += 1
        self.counters[metric_name].interval.start_time = datetime.datetime.now()  # FIXME

    def query_gauges(self):
        for metric_name, gauge_f in self.gauge_callbacks.items():
            self.gauges[metric_name].value.double_value = gauge_f()

    def push_metrics(self):
        series_request = monitoring_v3.CreateTimeSeriesRequest()
        series_request.name = self.client.common_project_path(self.project_id)
        counter_series = [self.make_time_series(metric_name, point, "CUMULATIVE")
                          for metric_name, point in self.counters.items()]
        gauge_series = [self.make_time_series(metric_name, point, "GAUGE")
                        for metric_name, point in self.gauges.items()]
        series_request.time_series = counter_series + gauge_series
        try:
            self.client.create_time_series(series_request)
        except GoogleAPIError as e:
            raise e
        self.counters = defaultdict(monitoring_v3.Point)
        self.gauges = defaultdict(monitoring_v3.Point)

    def make_time_series(self, metric_name, point, kind):
        gcp_series = monitoring_v3.TimeSeries()
        gcp_series.metric.type = f'{METRIC_BASE}/{metric_name}'
        gcp_series.metric_kind = kind
        self.resource_data.add_to(gcp_series)
        point.interval.end_time = datetime.datetime.now()
        gcp_series.points.append(point)
        return gcp_series


class ResourceData:
    def __init__(self, resource_type, cluster_name, namespace_name, location, container_name, pod_name):
        self.resource_type = resource_type
        self.cluster_name = cluster_name
        self.namespace_name = namespace_name
        self.location = location
        self.container_name = container_name
        self.pod_name = pod_name

    def add_to(self, gcp_series):
        gcp_series.resource.type = self.resource_type
        gcp_series.resource.labels['cluster_name'] = self.cluster_name
        gcp_series.resource.labels['namespace_name'] = self.namespace_name
        gcp_series.resource.labels['location'] = self.location
        gcp_series.resource.labels['container_name'] = self.container_name
        gcp_series.resource.labels['pod_name'] = self.pod_name
