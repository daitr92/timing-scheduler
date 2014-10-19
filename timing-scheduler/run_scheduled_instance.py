from crontab import CronTab
import sys

from nova.scheduler.timing_scheduler import TimingScheduler
from nova.context import RequestContext
from oslo.config import cfg
from nova import rpc
from nova import config

CONF = cfg.CONF
CONF.import_opt('scheduler_topic', 'nova.scheduler.rpcapi')

with open('/home/kahn/log.txt', 'w') as outfile:
    outfile.write('Start timing scheduler')

config.parse_args(sys.argv)

if len(sys.argv) >= 2:
    # cronjob_cmd = sys.argv[1]
    # cronjob_cmd = cronjob_cmd + " '" + cronjob_cmd + "'"
    # tab = CronTab()
    #
    # cronjob = tab.find_command(cronjob_cmd)
    # if cronjob:
    #     tab.remove_all(cronjob_cmd)
    #
    # tab.write()

    context_dict = eval(open('/home/kahn/context.txt', 'r').read())
    if context_dict is None:
        with open('/home/kahn/error.txt', 'w') as outfile:
            outfile.write('Can not open context file')
            exit(0)

    context = RequestContext.from_dict(context_dict)

    request_spec = eval(open('/home/kahn/request_spec.txt', 'r').read())
    if request_spec is None:
        with open('/home/kahn/error.txt', 'w') as outfile:
            outfile.write('Can not open request_spec file')
            exit(0)

    others_params_dict = eval(open('/home/kahn/others_params.txt', 'r').read())
    if others_params_dict is None:
        with open('/home/kahn/error.txt', 'w') as outfile:
            outfile.write('Can not open others_params file')
            exit(0)

    filter_properties = others_params_dict.get('filter_properties')
    requested_networks = others_params_dict.get('requested_networks')
    injected_files = others_params_dict.get('injected_files')
    admin_password = others_params_dict.get('admin_password')
    is_first_time = others_params_dict.get('is_first_time')
    legacy_bdm_in_spec = others_params_dict.get('legacy_bdm_in_spec')

    scheduler = TimingScheduler()
    scheduler.run_scheduled_instance(context,
                                     request_spec,
                                     filter_properties,
                                     requested_networks,
                                     injected_files, admin_password,
                                     is_first_time,
                                     legacy_bdm_in_spec)



