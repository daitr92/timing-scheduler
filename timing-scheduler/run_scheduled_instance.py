import sys

from nova.scheduler.timing_scheduler import TimingScheduler
from nova.context import RequestContext
from oslo.config import cfg
from nova import config
from bson.objectid import ObjectId

from pymongo import MongoClient

CONF = cfg.CONF
CONF.import_opt('scheduler_topic', 'nova.scheduler.rpcapi')

cli_opts = [
    cfg.StrOpt('object_id',
                short='o',
                default='Failed',
                help='Print more verbose output.')
]

CONF.register_cli_opts(cli_opts)

config.parse_args(sys.argv)

if len(sys.argv) >= 2:
    object_id = CONF.object_id

    print CONF.database.mongo_connection
    print CONF.database.mongo_database
    print CONF.database.mongo_collection

    client = MongoClient(CONF.database.mongo_connection)
    db = client[CONF.database.mongo_database]
    collection = db[CONF.database.mongo_collection]

    pending_instances = collection.find({"_id": ObjectId(object_id)})
    pending_instance = pending_instances.next()
    print pending_instance.get('context')

    # sys.exit(0)

    # context_dict = eval(open('/home/kahn/context.txt', 'r').read())
    # if context_dict is None:
    #     with open('/home/kahn/error.txt', 'w') as outfile:
    #         outfile.write('Can not open context file')
    #         exit(0)
    #
    # context = RequestContext.from_dict(context_dict)
    #
    # request_spec = eval(open('/home/kahn/request_spec.txt', 'r').read())
    # if request_spec is None:
    #     with open('/home/kahn/error.txt', 'w') as outfile:
    #         outfile.write('Can not open request_spec file')
    #         exit(0)
    #
    # others_params_dict = eval(open('/home/kahn/others_params.txt', 'r').read())
    # if others_params_dict is None:
    #     with open('/home/kahn/error.txt', 'w') as outfile:
    #         outfile.write('Can not open others_params file')
    #         exit(0)

    context_dict = eval(pending_instance.get('context'))
    context = RequestContext.from_dict(context_dict)

    request_spec = eval(pending_instance.get('request_spec'))

    others_params_dict = eval(pending_instance.get('other_params'))

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



