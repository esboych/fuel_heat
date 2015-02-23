#!/usr/bin/env python2
import sys
import time
import urllib2
import logging
import itertools
import collections
from functools import wraps
from argparse import ArgumentParser


import yaml
import prest
import netaddr
from keystoneclient import exceptions
from keystoneclient.v2_0 import Client as keystoneclient


logger = logging.getLogger("fuel_heat")


# HTTP Engine ---------------------------------------------------------------


class KeystoneAuth(prest.Urllib2HTTP_JSON):
    def __init__(self, root_url, creds, headers=None, echo=False,
                 admin_node_ip=None):
        super(KeystoneAuth, self).__init__(root_url,
                                           headers=headers,
                                           echo=echo)
        self.keystone_url = "http://{0}:5000/v2.0".format(admin_node_ip)
        self.keystone = keystoneclient(
            auth_url=self.keystone_url, **creds)
        self.refresh_token()

    def refresh_token(self):
        """Get new token from keystone and update headers"""
        try:
            self.keystone.authenticate()
            self.headers['X-Auth-Token'] = self.keystone.auth_token
        except exceptions.AuthorizationFailure:
            if logger is not None:
                logger.warning(
                    'Cant establish connection to keystone with url %s',
                    self.keystone_url)

    def do(self, method, path, params=None):
        """Do request. On 401 - refresh token"""
        try:
            return super(KeystoneAuth, self).do(method, path, params)
        except urllib2.HTTPError as e:
            if e.code == 401:
                if logger is not None:
                    logger.warning(
                        'Authorization failure: {0}'.format(e.read()))
                self.refresh_token()
                return super(KeystoneAuth, self).do(method, path, params)
            else:
                raise


# -------- UTILS -------------------------------------------------------------


def with_timeout(tout, message):
    def closure(func):
        @wraps(func)
        def closure2(*dt, **mp):
            ctime = time.time()
            etime = ctime + tout

            while ctime < etime:
                if func(*dt, **mp):
                    return
                sleep_time = ctime + 1 - time.time()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                ctime = time.time()
            raise RuntimeError("Timeout during " + message)
        return closure2
    return closure


# -------------------------------  ORM ----------------------------------------


class NodeList(list):
    """Class for filtering nodes through attributes"""
    allowed_roles = ['controller', 'compute', 'cinder', 'ceph-osd', 'mongo',
                     'zabbix-server']

    def __getattr__(self, name):
        if name in self.allowed_roles:
            return [node for node in self if name in node.roles]


class FuelInfo(prest.PRestBase):

    """Class represents Fuel installation info"""

    get_nodes = prest.GET('api/nodes')
    get_clusters = prest.GET('api/clusters')
    get_cluster = prest.GET('api/clusters/{id}')

    @property
    def nodes(self):
        """Get all fuel nodes"""
        return NodeList([Node(self.__connection__, **node) for node
                         in self.get_nodes()])

    @property
    def free_nodes(self):
        """Get unallocated nodes"""
        return NodeList([Node(self.__connection__, **node) for node in
                         self.get_nodes() if not node['cluster']])

    @property
    def clusters(self):
        """List clusters in fuel"""
        return [Cluster(self.__connection__, **cluster) for cluster
                in self.get_clusters()]


class Node(prest.PRestBase):
    """Represents node in Fuel"""

    get_info = prest.GET('/api/nodes/{id}')
    get_interfaces = prest.GET('/api/nodes/{id}/interfaces')
    update_interfaces = prest.PUT('/api/nodes/{id}/interfaces')

    def set_network_assigment(self, mapping):
        """Assings networks to interfaces
        :param mapping: list (dict) interfaces info
        """

        curr_interfaces = self.get_interfaces()

        network_ids = {}
        for interface in curr_interfaces:
            for net in interface['assigned_networks']:
                network_ids[net['name']] = net['id']

        # transform mappings
        new_assigned_networks = {}

        for dev_name, networks in mapping.items():
            new_assigned_networks[dev_name] = []
            for net_name in networks:
                nnet = {'name': net_name, 'id': network_ids[net_name]}
                new_assigned_networks[dev_name].append(nnet)

        # update by ref
        for dev_descr in curr_interfaces:
            if dev_descr['name'] in new_assigned_networks:
                nass = new_assigned_networks[dev_descr['name']]
                dev_descr['assigned_networks'] = nass

        self.update_interfaces(curr_interfaces, id=self.id)

    def set_node_name(self, name):
        """Update node name"""
        self.__connection__.put('api/nodes', [{'id': self.id, 'name': name}])

    def get_network_data(self):
        """Returns node network data"""
        node_info = self.get_info()
        return node_info.get('network_data')

    def get_roles(self):
        """Get node roles

        Returns: (roles, pending_roles)
        """
        node_info = self.get_info()
        return node_info.get('roles'), node_info.get('pending_roles')

    def get_ip(self, network='public'):
        """Get node ip

        :param network: network to pick
        """
        nets = self.get_network_data()
        for net in nets:
            if net['name'] == network:
                iface_name = net['dev']
                for iface in self.get_info()['meta']['interfaces']:
                    if iface['name'] == iface_name:
                        try:
                            return iface['ip']
                        except KeyError:
                            return netaddr.IPNetwork(net['ip']).ip
        raise Exception('Network %s not found' % network)


class Cluster(prest.PRestBase):
    """Class represents Cluster in Fuel"""

    add_node_call = prest.PUT('api/nodes')
    start_deploy = prest.PUT('api/clusters/{id}/changes')
    get_status = prest.GET('api/clusters/{id}')
    delete = prest.DELETE('api/clusters/{id}')
    get_tasks_status = prest.GET("api/tasks?cluster_id={id}")
    get_networks = prest.GET(
        'api/clusters/{id}/network_configuration/{net_provider}')

    get_attributes = prest.GET(
        'api/clusters/{id}/attributes')

    set_attributes = prest.PUT(
        'api/clusters/{id}/attributes')

    configure_networks = prest.PUT(
        'api/clusters/{id}/network_configuration/{net_provider}')

    _get_nodes = prest.GET('api/nodes?cluster_id={id}')

    def __init__(self, *dt, **mp):
        super(Cluster, self).__init__(*dt, **mp)
        self.nodes = NodeList()
        self.network_roles = {}

    def check_exists(self):
        """Check if cluster exists"""
        try:
            self.get_status()
            return True
        except urllib2.HTTPError as err:
            if err.code == 404:
                return False
            raise

    def get_nodes(self):
        for node_descr in self._get_nodes():
            yield Node(self.__connection__, **node_descr)

    def add_node(self, node, roles, interfaces=None):
        """Add node to cluster

        :param node: Node object
        :param roles: roles to assign
        :param interfaces: mapping iface name to networks
        """
        data = {}
        data['pending_roles'] = roles
        data['cluster_id'] = self.id
        data['id'] = node.id
        data['pending_addition'] = True

        if logger is not None:
            logger.debug("Adding node %s to cluster..." % node.id)

        self.add_node_call([data])
        self.nodes.append(node)

        if interfaces is not None:
            networks = {}
            for iface_name, params in interfaces.items():
                networks[iface_name] = params['networks']

            node.set_network_assigment(networks)

    def wait_operational(self, timeout):
        """Wait until cluster status operational"""
        def wo():
            status = self.get_status()['status']
            if status == "error":
                raise Exception("Cluster deploy failed")
            return self.get_status()['status'] == 'operational'
        with_timeout(timeout, "deploy cluster")(wo)()

    def deploy(self, timeout):
        """Start deploy and wait until all tasks finished"""
        logger.debug("Starting deploy...")
        self.start_deploy()

        self.wait_operational(timeout)

        def all_tasks_finished_ok(obj):
            ok = True
            for task in obj.get_tasks_status():
                if task['status'] == 'error':
                    raise Exception('Task execution error')
                elif task['status'] != 'ready':
                    ok = False
            return ok

        wto = with_timeout(timeout, "wait deployment finished")
        wto(all_tasks_finished_ok)(self)

    def set_networks(self, net_descriptions):
        """Update cluster networking parameters"""
        configuration = self.get_networks()
        current_networks = configuration['networks']
        parameters = configuration['networking_parameters']

        if net_descriptions.get('networks'):
            net_mapping = net_descriptions['networks']

            for net in current_networks:
                net_desc = net_mapping.get(net['name'])
                if net_desc:
                    net.update(net_desc)

        if net_descriptions.get('networking_parameters'):
            parameters.update(net_descriptions['networking_parameters'])

        self.configure_networks(**configuration)


def reflect_cluster(conn, cluster_id):
    """Returns cluster object by id"""
    c = Cluster(conn, id=cluster_id)
    c.nodes = NodeList(list(c.get_nodes()))
    return c


def get_all_nodes(conn):
    """Get all nodes from Fuel"""
    for node_desc in conn.get('api/nodes'):
        yield Node(conn, **node_desc)


def get_all_clusters(conn):
    """Get all clusters"""
    for cluster_desc in conn.get('api/clusters'):
        yield Cluster(conn, **cluster_desc)


def get_cluster_id(name, conn):
    """Get cluster id by name"""
    for cluster in get_all_clusters(conn):
        if cluster.name == name:
            if logger is not None:
                logger.debug('cluster name is %s' % name)
                logger.debug('cluster id is %s' % cluster.id)
            return cluster.id


def create_empty_cluster(conn, cluster_desc,
                         debug_mode=False,
                         use_ceph=False):
    """Create new cluster with configuration provided"""

    data = {}
    data['nodes'] = []
    data['tasks'] = []
    data['name'] = cluster_desc['name']
    data['release'] = cluster_desc['release']
    data['mode'] = cluster_desc.get('deployment_mode')

    net_prov = cluster_desc.get('net_provider')
    if net_prov == "neutron_vlan":
        data['net_provider'] = "neutron"
        data['net_segment_type'] = 'vlan'
    else:
        data['net_provider'] = net_prov

    params = conn.post(path='/api/clusters', params=data)
    cluster = Cluster(conn, **params)

    attributes = cluster.get_attributes()

    ed_attrs = attributes['editable']

    ed_attrs['common']['libvirt_type']['value'] = \
        cluster_desc.get('libvirt_type', 'kvm')

    if use_ceph:
        opts = ['ephemeral_ceph', 'images_ceph', 'images_vcenter']
        opts += ['iser', 'objects_ceph', 'volumes_ceph']
        opts += ['volumes_lvm', 'volumes_vmdk']

        for name in opts:
            val = ed_attrs['storage'][name]
            if val['type'] == 'checkbox':
                is_ceph = ('images_ceph' == name)
                is_ceph = is_ceph or ('volumes_ceph' == name)

                if is_ceph:
                    val['value'] = True
                else:
                    val['value'] = False

    cluster.set_attributes(attributes)

    return cluster


NodeGroup = collections.namedtuple('Node', ['roles', 'num', 'num_modif'])
RawNodeInfo = collections.namedtuple('RawNodeInfo', ['cpu', 'disk', 'node'])


def match_nodes(conn, cluster, max_nodes=None):
    node_groups = []

    for node_group in cluster:
        rroles, rcount = node_group.split(",")

        rroles = rroles.strip()
        rcount = rcount.strip()

        roles = [role.strip() for role in rroles.split('+')]

        if rcount.endswith("+"):
            node_groups.append(NodeGroup(roles, int(rcount[:-1]), '+'))
        else:
            node_groups.append(NodeGroup(roles, int(rcount), None))

    min_nodes = sum(node_group.num for node_group in node_groups)

    if max_nodes is not None and max_nodes < min_nodes:
        templ = "max_nodes ({0!r}) < min_nodes ({1!r})"
        raise ValueError(templ.format(max_nodes, min_nodes))

    for node_group in node_groups:
        logger.info("Node : {0}".format(node_group))

    controller_only = sum(node_group.num for node_group in node_groups
                          if ['controller'] == node_group.roles)

    while True:
        raw_nodes = [raw_node for raw_node in get_all_nodes(conn)
                     if raw_node.cluster is None]

        if len(raw_nodes) < min_nodes:
            templ = "Waiting till {} nodes will be available"
            logger.info(templ.format(min_nodes))
            time.sleep(10)
            continue
        break

    if len(raw_nodes) <= 1:
        raise ValueError("Nodes amount should be not less, than 2")

    cpu_disk = []
    for raw_node in raw_nodes:
        info = raw_node.get_info()

        cpu_count = int(info['meta']['cpu']['real'])
        disk_size = 0

        for disk in info['meta']['disks']:
            disk_size += int(disk['size'])

        cpu_disk.append(RawNodeInfo(cpu_count, disk_size, raw_node))

    cpu_disk.sort()

    # least performant node - controllers
    for idx, node_info in enumerate(cpu_disk[:controller_only]):
        descr = {'roles': ["controller"],
                 'name': "controller_{}".format(idx)}
        yield (descr, node_info.node)

    cpu_disk = cpu_disk[controller_only:]
    non_c_node_groups = [node_group for node_group in node_groups
                         if ['controller'] != node_group.roles]

    def make_name(group, idxs={}):
        name_templ = "_".join(group.roles)
        idx = idxs.get(name_templ, 0)
        idxs[name_templ] = idx + 1
        return "{0}_{1}".format(name_templ, idx)

    compute_nodes = [node_group for node_group in non_c_node_groups
                     if 'compute' in node_group.roles]

    for node_group in compute_nodes:
        for _ in range(node_group.num):
            name = make_name(node_group)
            descr = {'roles': node_group.roles,
                     'name': name}
            yield (descr, cpu_disk.pop().node)

    data_nodes = [node_group for node_group in non_c_node_groups
                  if 'compute' not in node_group.roles]

    for node_group in data_nodes:
        for _ in range(node_group.num):
            name = make_name(node_group)
            descr = {'roles': node_group.roles,
                     'name': name}
            yield (descr, cpu_disk.pop().node)

    strechable_node_groups = [node_group for node_group in node_groups
                              if node_group.num_modif == '+']

    if len(strechable_node_groups) != 0:
        cycle_over = enumerate(itertools.cycle(strechable_node_groups),
                               min_nodes)

        nums = {id(node_group): node_group.num
                for node_group in strechable_node_groups}

        for selected_nodes, node_group in cycle_over:
            if cpu_disk == [] or selected_nodes == max_nodes:
                break

            name = make_name(node_group, nums[id(node_group)])
            nums[id(node_group)] += 1
            descr = {'roles': node_group.roles,
                     'name': name}
            yield (descr, cpu_disk.pop().node)


def str2ip_range(ip_str):
    ip1, ip2 = ip_str.split("-")
    return [ip1.strip(), ip2.strip()]


def get_net_cfg_ref(network_config, network_name):
    for net in network_config['networks']:
        if net['name'] == network_name:
            return net
    raise KeyError("Network {0!r} not found".format(network_name))


def set_networks_params(cluster, net_settings):
    configuration = cluster.get_networks()
    curr_config = configuration['networking_parameters']

    if 'floating' in net_settings:
        curr_config['floating_ranges'] = \
            [str2ip_range(net_settings['floating'])]

    fields = ['net_manager', 'net_l23_provider', 'vlan_range']

    for field in fields:
        if field in net_settings:
            curr_config[field] = net_settings[field]

    if 'public' in net_settings:
        pub_settings = net_settings['public']
        for net in configuration['networks']:
            if net['name'] == 'public':

                if 'ip_ranges' in pub_settings:
                    ip_range = str2ip_range(pub_settings['ip_ranges'])
                    net['ip_ranges'] = [ip_range]

                if 'cidr' in pub_settings:
                    net['cidr'] = pub_settings['cidr']

                if 'gateway' in pub_settings:
                    net['gateway'] = pub_settings['gateway']

    if 'storage' in net_settings:
        if 'vlan' in net_settings['storage']:
            net = get_net_cfg_ref(configuration, 'storage')
            net['vlan_start'] = net_settings['storage']['vlan']

    if 'management' in net_settings:
        if 'vlan' in net_settings['management']:
            net = get_net_cfg_ref(configuration, 'management')
            net['vlan_start'] = net_settings['management']['vlan']

    cluster.configure_networks(**configuration)


def create_cluster(conn, cluster):
    nodes_iter = match_nodes(conn, cluster['nodes'])

    use_ceph = False

    if 'nodes' in cluster:
        for node_group in cluster['nodes']:
            if 'ceph-osd' in node_group:
                use_ceph = True

    if cluster.get('storage_type', None) == 'ceph':
        use_ceph = True

    if use_ceph:
        logger.info("Will use ceph as storage")

    logger.info("Creating empty cluster")
    cluster_obj = create_empty_cluster(conn, cluster,
                                       use_ceph=use_ceph)

    try:
        if 'network' in cluster:
            logger.info("Setting network parameters")
            set_networks_params(cluster_obj, cluster['network'])

        for node_desc, node in nodes_iter:
            node.set_node_name(node_desc['name'])
            templ = "Adding node {} with roles {}"

            logger.info(templ.format(node.name, ",".join(node_desc['roles'])))

            cluster_obj.add_node(node, node_desc['roles'])
    except:
        cluster_obj.delete()
        raise

    return cluster_obj


def login(fuel_url, creds):
    if fuel_url.endswith("/"):
        fuel_url = fuel_url[:-1]

    admin_node_ip = fuel_url.split('/')[-1].split(':')[0]
    username, password, tenant_name = creds.split(":")
    keyst_creds = {'username': username,
                   'password': password,
                   'tenant_name': tenant_name}
    return KeystoneAuth(fuel_url,
                        creds=keyst_creds,
                        echo=True,
                        admin_node_ip=admin_node_ip)


def parse_command_line(argv):
    parser = ArgumentParser("usage: {0} [options]".format(argv[0]))

    parser.add_argument('-a', '--auth',
                        help='keystone credentials in format '
                             'tenant_name:username:password',
                        dest="auth", default='admin:admin:admin')

    parser.add_argument('-u', '--fuelurl', help="fuel rest url",
                        dest='fuelurl', required=True)

    parser.add_argument('config_file',
                        help='yaml configuration file',
                        metavar="CLUSTER_CONFIG")

    parser.add_argument('-d', '--debug',
                        help='allow debug logging',
                        action="store_true")

    return parser.parse_args(argv[1:])


def main(argv=None):
    if argv is None:
        argv = sys.argv

    args = parse_command_line(argv)

    if args.debug:
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)

        log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
        formatter = logging.Formatter(log_format,
                                      "%H:%M:%S")
        ch.setFormatter(formatter)

    conn = login(args.fuelurl, args.auth)
    cluster = yaml.load(open(args.config_file).read())
    fuel = FuelInfo(conn)

    for cluster_obj in fuel.clusters:
        if cluster_obj.name == cluster['name']:
            cluster_obj.delete()
            wd = with_timeout(60, "Wait cluster deleted")
            wd(lambda co: not co.check_exists())(cluster_obj)

    create_cluster(conn, cluster)

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
