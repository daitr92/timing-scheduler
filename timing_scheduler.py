# Copyright (c) 2011 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
The FilterScheduler is for creating instances locally.
You can customize this scheduler by specifying your own Host Filters and
Weighing Functions.
"""

import random

from oslo.config import cfg

from nova.compute import rpcapi as compute_rpcapi
from nova import exception
from nova.objects import instance_group as instance_group_obj
# from nova.openstack.common.gettextutils import _
from nova.i18n import _
from nova.openstack.common import log as logging
from nova.pci import pci_request
from nova import rpc
from nova.scheduler import driver
from nova.scheduler import scheduler_options
from nova.scheduler import utils as scheduler_utils

from novaclient.v1_1 import client
from novaclient.extension import Extension
from novaclient.v1_1.contrib import  scheduler_partner

from crontab import CronTab
import datetime

from pymongo import MongoClient
from pprint import pprint
import os
import subprocess
import nova.db.api as DbAPI
import pdb
from syslog import LOG_MASK
from time import strptime


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


filter_scheduler_opts = [
    cfg.IntOpt('scheduler_host_subset_size',
               default=1,
               help='New instances will be scheduled on a host chosen '
                    'randomly from a subset of the N best hosts. This '
                    'property defines the subset size that a host is '
                    'chosen from. A value of 1 chooses the '
                    'first host returned by the weighing functions. '
                    'This value must be at least 1. Any value less than 1 '
                    'will be ignored, and 1 will be used instead')
]

CONF.register_opts(filter_scheduler_opts)

db_group = cfg.OptGroup(name='database',
                        title='Databse Configuration Group')

db_opts = [
    cfg.StrOpt('mongo_connection', default='mongodb://nova:openstack@localhost/nova'),
    cfg.StrOpt('mongo_database', default='nova'),
    cfg.StrOpt('mongo_collection', default='pendinginstance')
]

CONF.register_group(db_group)
CONF.register_opts(db_opts, db_group)


class TimingScheduler(driver.Scheduler):
    """Scheduler that can be used for filtering and weighing."""
    def __init__(self, *args, **kwargs):
        super(TimingScheduler, self).__init__(*args, **kwargs)
        self.options = scheduler_options.SchedulerOptions()
        self.compute_rpcapi = compute_rpcapi.ComputeAPI()
        self.notifier = rpc.get_notifier('scheduler')
        #pdb.set_trace()

    def schedule_run_instance(self, context, request_spec,
                              admin_password, injected_files,
                              requested_networks, is_first_time,
                              filter_properties, legacy_bdm_in_spec):
        """This method is called from nova.compute.api to provision
        an instance.  We first create a build plan (a list of WeightedHosts)
        and then provision.

        Returns a list of the instances created.
        """
        print 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'

        scheduler_hints = filter_properties.get('scheduler_hints')
        partner_shortname = scheduler_hints['partner']
        # partner = DbAPI.partners_get_by_shortname(context, partner_shortname)
        partners = DbAPI.partners_get_all(context)

        if not partners:
            print "There is no partner!"
            return

        for partner in partners:
            print "Trying to send request to %s" % partner.shortname
            partner_client = client.Client(partner.username, partner.password, 'demo', partner.auth_url,
                                   service_type='compute', extensions=[
                                        Extension('scheduler_partner', scheduler_partner)
                                    ])

            data = {}
            data['flavor'] = request_spec['instance_type']['flavorid']
            data['num_instances'] = scheduler_hints['num_instances']

            result = partner_client.scheduler_partner.create(data)
            if not result or u'success' not in result:
                print "%s is now offline!" % partner.shortname
                continue

            if result[u'success'] == 1:
                DbAPI.partners_update(context, partner.shortname, {
                    'requested': result['points']
                })

                break

            print result

        return


        # payload = dict(request_spec=request_spec)
        # self.notifier.info(context, 'scheduler.run_instance.start', payload)
        #
        # instance_uuids = request_spec.get('instance_uuids')
        # LOG.info(_("Attempting to build %(num_instances)d instance(s) "
        #             "uuids: %(instance_uuids)s"),
        #           {'num_instances': len(instance_uuids),
        #            'instance_uuids': instance_uuids})
        # LOG.debug(_("Request Spec: %s") % request_spec)

        # client = MongoClient(CONF.database.mongo_connection)
        # db = client[CONF.database.mongo_database]
        # collection = db[CONF.database.mongo_collection]

        # print CONF.database.mongo_connection
        # print CONF.database.mongo_database
        # print CONF.database.mongo_collection

        other_params = {
            'filter_properties': filter_properties,
            'requested_networks': requested_networks,
            'injected_files': injected_files,
            'admin_password': admin_password,
            'is_first_time': is_first_time,
            'legacy_bdm_in_spec': legacy_bdm_in_spec
        }

        data = {}
        data['instance_uuids'] = request_spec['instance_uuids']
        # data['context'] = str(context.to_dict())
        data['request_spec'] = str(request_spec)
        data['other_params'] = str(other_params)
        data['pending'] = True

        res_client = client.Client('admin', '8954', 'demo', 'http://localhost:35357/v2.0',
                                   service_type='compute', extensions=[
                                        Extension('scheduler_partner', scheduler_partner)
                                    ])

        result = res_client.scheduler_partner.create(data)
        print(result.success)

        # id_object = collection.insert(data)
        #
        # scheduler_hints = filter_properties.get('scheduler_hints')
        # start_time = datetime.datetime.now()
        # LOG.debug(_("Now: %s") % start_time)
        # if scheduler_hints:
        #     start_time_str = scheduler_hints['start_time'] #format Y-m-d:H-M-S
        #     LOG.debug(_("Scheduler Time string : %s") % start_time_str)
        #     if start_time_str:
        #         start_time = datetime.datetime.strptime(start_time_str,"%Y-%m-%d:%H:%M:%S")
        #         LOG.debug(_("Scheduler Time At: %s") % start_time)
        #
        # self._add_cron_tab(start_time, id_object.__str__())

    def _add_cron_tab(self, scheduled_time, object_id):

        LOG.debug(_("Add new cron tab at: %s") % scheduled_time )
        cron = CronTab()
        python_exec = (subprocess.check_output(['which','python'])).rstrip('\n')
        cwd = os.path.dirname(__file__)
        scheduler_path = cwd + "/timing-scheduler/run_scheduled_instance.py"
        config_file_path = self._get_config_file_path()
        config_file_params = " --config-file /etc/nova/nova.conf"
        if config_file_path:
            config_file_params = '--config-file ' + config_file_path
        LOG.debug(_("Config file params %s") % config_file_params )

        object_id_params = "--object_id %s" % object_id

        cmd = python_exec + " "+ \
              scheduler_path + " " + \
              config_file_params + " " + \
              object_id_params
        LOG.debug(_("Cron job cmd %s") % cmd )

        job = cron.new(command=cmd)
        job.hour.on(scheduled_time.hour)
        job.minute.on(scheduled_time.minute)
        job.day.on(scheduled_time.day)
        job.month.on(scheduled_time.month)

        cron.write()


    def _get_config_file_path(self):
        config_file_path = self._get_first_default_config_file()
        config_file_path = self._get_config_file_from_arg()
        return config_file_path

    def _get_first_default_config_file(selfs):
        config_file_path = ""
        for default_config_file in CONF.default_config_files:
            if os.path.isfile(default_config_file):
                config_file_path = default_config_file
                break
        return  config_file_path;

    def _get_config_file_from_arg(self):
        config_file_path = ""
        for index,arg in enumerate(CONF._args):
            if arg.startswith('--config-file='):
                head,sep,tail = arg.partition("=")
                config_file_path = tail
                break
            elif arg == '--config-file':
                config_file_path = CONF._args[index+1]

        LOG.debug(_("Config file path: %s") % config_file_path )
        return config_file_path

    def run_scheduled_instance(self, context,
                                         request_spec,
                                         filter_properties,
                                         requested_networks,
                                         injected_files, admin_password,
                                         is_first_time,
                                         legacy_bdm_in_spec):
        # pdb.set_trace()

        payload = dict(request_spec=request_spec)
        self.notifier.info(context, 'scheduler.run_instance.start', payload)

        instance_uuids = request_spec.get('instance_uuids')
        LOG.info(_("Attempting to build %(num_instances)d instance(s) "
                    "uuids: %(instance_uuids)s"),
                  {'num_instances': len(instance_uuids),
                   'instance_uuids': instance_uuids})
        LOG.debug(_("Request Spec: %s") % request_spec)

        weighed_hosts = self._schedule(context, request_spec,
                                       filter_properties, instance_uuids)

        # NOTE: Pop instance_uuids as individual creates do not need the
        # set of uuids. Do not pop before here as the upper exception
        # handler fo NoValidHost needs the uuid to set error state
        instance_uuids = request_spec.pop('instance_uuids')

        # NOTE(comstud): Make sure we do not pass this through.  It
        # contains an instance of RpcContext that cannot be serialized.
        filter_properties.pop('context', None)

        for num, instance_uuid in enumerate(instance_uuids):
            request_spec['instance_properties']['launch_index'] = num

            try:
                try:
                    weighed_host = weighed_hosts.pop(0)
                    LOG.info(_("Choosing host %(weighed_host)s "
                                "for instance %(instance_uuid)s"),
                              {'weighed_host': weighed_host,
                               'instance_uuid': instance_uuid})
                except IndexError:
                    raise exception.NoValidHost(reason="")

                self._provision_resource(context, weighed_host,
                                         request_spec,
                                         filter_properties,
                                         requested_networks,
                                         injected_files, admin_password,
                                         is_first_time,
                                         instance_uuid=instance_uuid,
                                         legacy_bdm_in_spec=legacy_bdm_in_spec)
            except Exception as ex:
                # NOTE(vish): we don't reraise the exception here to make sure
                #             that all instances in the request get set to
                #             error properly
                driver.handle_schedule_error(context, ex, instance_uuid,
                                             request_spec)
            # scrub retry host list in case we're scheduling multiple
            # instances:
            retry = filter_properties.get('retry', {})
            retry['hosts'] = []

        self.notifier.info(context, 'scheduler.run_instance.end', payload)

    def select_destinations(self, context, request_spec, filter_properties):
        """Selects a filtered set of hosts and nodes."""
        num_instances = request_spec['num_instances']
        instance_uuids = request_spec.get('instance_uuids')
        selected_hosts = self._schedule(context, request_spec,
                                        filter_properties, instance_uuids)

        # Couldn't fulfill the request_spec
        if len(selected_hosts) < num_instances:
            raise exception.NoValidHost(reason='')

        dests = [dict(host=host.obj.host, nodename=host.obj.nodename,
                      limits=host.obj.limits) for host in selected_hosts]
        return dests

    def _provision_resource(self, context, weighed_host, request_spec,
            filter_properties, requested_networks, injected_files,
            admin_password, is_first_time, instance_uuid=None,
            legacy_bdm_in_spec=True):
        """Create the requested resource in this Zone."""
        # NOTE(vish): add our current instance back into the request spec
        request_spec['instance_uuids'] = [instance_uuid]
        payload = dict(request_spec=request_spec,
                       weighted_host=weighed_host.to_dict(),
                       instance_id=instance_uuid)
        self.notifier.info(context,
                           'scheduler.run_instance.scheduled', payload)

        # Update the metadata if necessary
        scheduler_hints = filter_properties.get('scheduler_hints') or {}
        try:
            updated_instance = driver.instance_update_db(context,
                                                         instance_uuid)
        except exception.InstanceNotFound:
            LOG.warning(_("Instance disappeared during scheduling"),
                        context=context, instance_uuid=instance_uuid)

        else:
            scheduler_utils.populate_filter_properties(filter_properties,
                    weighed_host.obj)

            self.compute_rpcapi.run_instance(context,
                    instance=updated_instance,
                    host=weighed_host.obj.host,
                    request_spec=request_spec,
                    filter_properties=filter_properties,
                    requested_networks=requested_networks,
                    injected_files=injected_files,
                    admin_password=admin_password, is_first_time=is_first_time,
                    node=weighed_host.obj.nodename,
                    legacy_bdm_in_spec=legacy_bdm_in_spec)

    def _get_configuration_options(self):
        """Fetch options dictionary. Broken out for testing."""
        return self.options.get_configuration()

    def populate_filter_properties(self, request_spec, filter_properties):
        """Stuff things into filter_properties.  Can be overridden in a
        subclass to add more data.
        """
        # Save useful information from the request spec for filter processing:
        project_id = request_spec['instance_properties']['project_id']
        os_type = request_spec['instance_properties']['os_type']
        filter_properties['project_id'] = project_id
        filter_properties['os_type'] = os_type
        pci_requests = pci_request.get_pci_requests_from_flavor(
            request_spec.get('instance_type') or {})
        if pci_requests:
            filter_properties['pci_requests'] = pci_requests

    def _max_attempts(self):
        max_attempts = CONF.scheduler_max_attempts
        if max_attempts < 1:
            raise exception.NovaException(_("Invalid value for "
                "'scheduler_max_attempts', must be >= 1"))
        return max_attempts

    def _log_compute_error(self, instance_uuid, retry):
        """If the request contained an exception from a previous compute
        build/resize operation, log it to aid debugging
        """
        exc = retry.pop('exc', None)  # string-ified exception from compute
        if not exc:
            return  # no exception info from a previous attempt, skip

        hosts = retry.get('hosts', None)
        if not hosts:
            return  # no previously attempted hosts, skip

        last_host, last_node = hosts[-1]
        LOG.error(_('Error from last host: %(last_host)s (node %(last_node)s):'
                    ' %(exc)s'),
                  {'last_host': last_host,
                   'last_node': last_node,
                   'exc': exc},
                  instance_uuid=instance_uuid)

    def _populate_retry(self, filter_properties, instance_properties):
        """Populate filter properties with history of retries for this
        request. If maximum retries is exceeded, raise NoValidHost.
        """
        max_attempts = self._max_attempts()
        force_hosts = filter_properties.get('force_hosts', [])
        force_nodes = filter_properties.get('force_nodes', [])

        if max_attempts == 1 or force_hosts or force_nodes:
            # re-scheduling is disabled.
            return

        retry = filter_properties.pop('retry', {})
        # retry is enabled, update attempt count:
        if retry:
            retry['num_attempts'] += 1
        else:
            retry = {
                'num_attempts': 1,
                'hosts': []  # list of compute hosts tried
            }
        filter_properties['retry'] = retry

        instance_uuid = instance_properties.get('uuid')
        self._log_compute_error(instance_uuid, retry)

        if retry['num_attempts'] > max_attempts:
            msg = (_('Exceeded max scheduling attempts %(max_attempts)d for '
                     'instance %(instance_uuid)s')
                   % {'max_attempts': max_attempts,
                      'instance_uuid': instance_uuid})
            raise exception.NoValidHost(reason=msg)

    @staticmethod
    def _setup_instance_group(context, filter_properties):
        update_group_hosts = False
        scheduler_hints = filter_properties.get('scheduler_hints') or {}
        group_hint = scheduler_hints.get('group', None)
        if group_hint:
            group = instance_group_obj.InstanceGroup.get_by_hint(context,
                        group_hint)
            policies = set(('anti-affinity', 'affinity'))
            if any((policy in policies) for policy in group.policies):
                update_group_hosts = True
                filter_properties.setdefault('group_hosts', set())
                user_hosts = set(filter_properties['group_hosts'])
                group_hosts = set(group.get_hosts(context))
                filter_properties['group_hosts'] = user_hosts | group_hosts
                filter_properties['group_policies'] = group.policies
        return update_group_hosts

    def _schedule(self, context, request_spec, filter_properties,
                  instance_uuids=None):
        """Returns a list of hosts that meet the required specs,
        ordered by their fitness.
        """
        elevated = context.elevated()
        instance_properties = request_spec['instance_properties']
        instance_type = request_spec.get("instance_type", None)

        update_group_hosts = self._setup_instance_group(context,
                filter_properties)

        config_options = self._get_configuration_options()

        # check retry policy.  Rather ugly use of instance_uuids[0]...
        # but if we've exceeded max retries... then we really only
        # have a single instance.
        properties = instance_properties.copy()
        if instance_uuids:
            properties['uuid'] = instance_uuids[0]
        self._populate_retry(filter_properties, properties)

        filter_properties.update({'context': context,
                                  'request_spec': request_spec,
                                  'config_options': config_options,
                                  'instance_type': instance_type})

        self.populate_filter_properties(request_spec,
                                        filter_properties)

        # Find our local list of acceptable hosts by repeatedly
        # filtering and weighing our options. Each time we choose a
        # host, we virtually consume resources on it so subsequent
        # selections can adjust accordingly.

        # Note: remember, we are using an iterator here. So only
        # traverse this list once. This can bite you if the hosts
        # are being scanned in a filter or weighing function.
        hosts = self._get_all_host_states(elevated)

        selected_hosts = []
        if instance_uuids:
            num_instances = len(instance_uuids)
        else:
            num_instances = request_spec.get('num_instances', 1)
        for num in xrange(num_instances):
            # Filter local hosts based on requirements ...
            hosts = self.host_manager.get_filtered_hosts(hosts,
                    filter_properties, index=num)
            if not hosts:
                # Can't get any more locally.
                break

            LOG.debug(_("Filtered %(hosts)s"), {'hosts': hosts})

            weighed_hosts = self.host_manager.get_weighed_hosts(hosts,
                    filter_properties)

            LOG.debug(_("Weighed %(hosts)s"), {'hosts': weighed_hosts})

            scheduler_host_subset_size = CONF.scheduler_host_subset_size
            if scheduler_host_subset_size > len(weighed_hosts):
                scheduler_host_subset_size = len(weighed_hosts)
            if scheduler_host_subset_size < 1:
                scheduler_host_subset_size = 1

            chosen_host = random.choice(
                weighed_hosts[0:scheduler_host_subset_size])
            selected_hosts.append(chosen_host)

            # Now consume the resources so the filter/weights
            # will change for the next instance.
            chosen_host.obj.consume_from_instance(instance_properties)
            if update_group_hosts is True:
                filter_properties['group_hosts'].add(chosen_host.obj.host)
        return selected_hosts

    def _get_all_host_states(self, context):
        """Template method, so a subclass can implement caching."""
        return self.host_manager.get_all_host_states(context)
