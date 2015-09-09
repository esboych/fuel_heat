#!/usr/bin/env python2
import json
import os
import sys
import time
import urllib2
import logging
import itertools
import collections
from functools import wraps
from argparse import ArgumentParser
import paramiko

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
    get_disks = prest.GET('/api/nodes/{id}/disks')
    update_disks = prest.PUT('/api/nodes/{id}/disks')

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

    def update_disk_volumes(self, disks):
        old_disks = self.get_disks()


        for i in range(len(disks)):
            old_disk = old_disks[i]
            new_disk = disks[i]
            old_disk["volumes"] = new_disk["volumes"]

        self.update_disks(old_disks, id=self.id)



class Cluster(prest.PRestBase):
    """Class represents Cluster in Fuel"""

    networks = {}  #j: dict with nets and their ids

    add_node_call = prest.PUT('api/nodes')
    start_deploy = prest.PUT('api/clusters/{id}/changes')
    get_status = prest.GET('api/clusters/{id}')
    delete = prest.DELETE('api/clusters/{id}')
    get_tasks_status = prest.GET("api/tasks?cluster_id={id}")
    get_networks = prest.GET(
        'api/clusters/{id}/network_configuration/{net_provider}')

    #j: get Fuel release
    get_releases = prest.GET(
        'api/releases')
    #j: getting interfaces info
    get_interfaces = prest.GET(
        'api/nodes/{id}/interfaces')

    get_attributes = prest.GET(
        'api/clusters/{id}/attributes')

    set_attributes = prest.PUT(
        'api/clusters/{id}/attributes')

    configure_networks = prest.PUT(
        'api/clusters/{id}/network_configuration/{net_provider}')

    #j: putting interfaces info
    configure_interfaces = prest.PUT(
        'api/nodes/{id}/interfaces')

    get_disks = prest.GET('/api/nodes/{id}/disks')
    update_disks = prest.PUT('/api/nodes/{id}/disks')

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
    net_segment_type = cluster_desc.get('net_segment_type')


    if net_prov == "neutron_vlan":
        data['net_provider'] = "neutron"
        data['net_segment_type'] = 'vlan'
    #j: added Nova case
    if net_prov == "nova_network":
        data['net_provider'] = "nova_network"

    else:
        data['net_provider'] = net_prov
        data['net_segment_type'] = net_segment_type

    #j: check what sent to api/clusters:
    logger.debug("POST api/clusters:", data)

    params = conn.post(path='/api/clusters', params=data)
    cluster = Cluster(conn, **params)

    #j: check what response api/clusters:
    logger.debug("POST api/clusters response:", params)

    attributes = cluster.get_attributes()
    logger.debug("Attributes are:", attributes)

    ed_attrs = attributes['editable']
    logger.debug("Attributes edited 1")

    ed_attrs['common']['libvirt_type']['value'] = \
        cluster_desc.get('libvirt_type', 'kvm')
    logger.debug("Attributes edited 2")

    #j: define provision type: "image"(IBP) or "cobbler"(Classic)
    logger.debug("Provision type is:", cluster_desc.get('provision'))
    #ed_attrs['provision']['method']['value'] = cluster_desc.get('provision') #27.08.2015 helped but?? neede to explore


    if use_ceph:
        opts = ['ephemeral_ceph', 'images_ceph', 'images_vcenter']
        opts += ['iser', 'objects_ceph', 'volumes_ceph']
        opts += ['volumes_lvm']

        for name in opts:
            val = ed_attrs['storage'][name]
            if val['type'] == 'checkbox':
                is_ceph = ('images_ceph' == name)
                is_ceph = is_ceph or ('volumes_ceph' == name)

                if is_ceph:
                    val['value'] = True
                else:
                    val['value'] = False

    logger.debug("Setting attributes")
    cluster.set_attributes(attributes)
    logger.debug("Setting attributes done")
    logger.debug("New config data is:", cluster.get_attributes())

    return cluster


NodeGroup = collections.namedtuple('Node', ['roles', 'num', 'num_modif'])
RawNodeInfo = collections.namedtuple('RawNodeInfo', ['cpu', 'disk', 'ram', 'mac', 'node']) #j: added 'mac' parameter


def match_nodes(conn, cluster, max_nodes=None):

    logger.info(str(cluster))
    for node_mac in cluster:
        logger.info(node_mac)
    min_nodes = len(cluster)
    logger.info(min_nodes)

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

    for raw_node in raw_nodes:
        info = raw_node.get_info()

        #j:
#        print

        mac = str(info['mac'])

        if mac in cluster:
            templ = "Matched node. MAC: {}, Name: {}"
            logger.info(templ.format(info['mac'], info['name']))
            node_description = cluster[mac]
            yield (node_description, raw_node)



def str2ip_range(ip_str):
    ip1, ip2 = ip_str.split("-")
    return [ip1.strip(), ip2.strip()]

#j: bond slaves formatting
def str2bond_slaves(slv_str):
    slv1, slv2 = slv_str.split(",")
    logger.debug("!!! slave1, slaves2 are:" , slv1, slv2)
    #str = '[{"name":"' + slv1 + '"},{"name":"' + slv2 + '"}]'
    str = '[{"name":"eth1"},{"name":"eth0"}]'
    logger.debug("!!! str:" , str)
    return str


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

    #j: fixed VLAN id for Nova network
    if 'fixed' in net_settings:
        curr_config['fixed_networks_vlan_start'] = net_settings['fixed']['vlan']

    fields = ['net_manager', 'net_l23_provider', 'vlan_range']

    for field in fields:
        if field in net_settings:
            curr_config[field] = net_settings[field]

    if 'public' in net_settings:
        pub_settings = net_settings['public']

        logger.debug("configuration['networks']", configuration['networks'])

        for net in configuration['networks']:

            #j detect network ids and save in Cluster object
            logger.debug(" net name", net['name'], "!!!net id: ", net["id"])
            cluster.networks[net['name']] = net['id']

            if net['name'] == 'public':

                if 'ip_ranges' in pub_settings:
                    ip_range = str2ip_range(pub_settings['ip_ranges'])
                    net['ip_ranges'] = [ip_range]

                if 'cidr' in pub_settings:
                    net['cidr'] = pub_settings['cidr']

                if 'gateway' in pub_settings:
                    net['gateway'] = pub_settings['gateway']
    #j:
    logger.debug(" set_networks_params():  networks[] in cluster: ", cluster.networks)

    if 'storage' in net_settings:
        if 'vlan' in net_settings['storage']:
            net = get_net_cfg_ref(configuration, 'storage')
            net['vlan_start'] = net_settings['storage']['vlan']

    if 'management' in net_settings:
        if 'vlan' in net_settings['management']:
            net = get_net_cfg_ref(configuration, 'management')
            net['vlan_start'] = net_settings['management']['vlan']
    #j: added as public became tagged with 200 vlan
    if 'public' in net_settings:
        if 'vlan' in net_settings['public']:
            net = get_net_cfg_ref(configuration, 'public')
            net['vlan_start'] = net_settings['public']['vlan']

    logger.debug("!!! NET configuration" , configuration)
    cluster.configure_networks(**configuration)
    #j: check what's configured:
    configuration = cluster.get_networks()
    logger.debug("new network configuration: ", configuration['networks'])


#j: configure interfaces
def set_interface_params(cluster, node_desc, node):

    #j: net ids
    network_ids = {
        "public": cluster.networks['public'],
        "management": cluster.networks['management'],
        "storage": cluster.networks['storage'],
        "private": cluster.networks.get('private', None),
        "fixed": cluster.networks.get('fixed', None),
        "fuelweb_admin": cluster.networks['fuelweb_admin']
    }

    configuration = cluster.get_interfaces(id = node.id)

    for interface in configuration:
        iname = interface['name']
        templ = "Interface name: {}\n Interface dump: {}"
        logger.debug(templ.format(iname, interface))
        if iname in node_desc['interfaces']:
            networks = node_desc['interfaces'][iname]
            templ = "networks_config: {}"
            logger.info(templ.format(networks))
            interface['assigned_networks'] = map(lambda x:dict(id=network_ids[x], name=x), networks)
            templ = "map_assigned_networks: {}"
            logger.info(templ.format(interface['assigned_networks']))
        else:
            interface['assigned_networks'] = []


        templ = "Node {} Interface {}, speed {} Mbps"
        logger.info(templ.format(node_desc['name'], interface['name'], interface['max_speed']))

    cluster.configure_interfaces(configuration, id = node.id)

#    print " Interfaces config: ", configuration

'''
    ## In Fuel 7.0 there are "floating" ethxx names of 10Gbps cards
    #Forming array of 2 such interfaces
    bond_arr = []
    admin_arr = []
    for interface in configuration:
        if interface['max_speed'] == 10000:
 #           print "10GBps found!: ", interface
            bond_arr.append(interface['name'])
#    print "bond_arr: ", bond_arr


    net_configuration = cluster.get_networks()
    #print 'configuration:', configuration
    print " 'management' net configuration:", net_configuration['networks'][2] # management
    print " Node: ", node
    net_id = net_configuration['networks'][2]["id"]
    #curr_config = configuration[0]

    compute_macs = ['0c:c4:7a:06:47:ea', '0c:c4:7a:0c:93:08']
    """
    if node.mac in compute_macs:
        print "Compute detected, mac:",node.mac
        if "bond" in intf_settings:
            bond_settings = intf_settings['bond']
            slaves = str2bond_slaves(bond_settings['slaves'])
            configuration.append({
                "name": "ovs-bond0",
                "state": 'null',
                "mac": 'null',
                "mode": "lacp-balance-tcp",
                "slaves": [
                    {
                        "name": "eth1"
                    },
                    {
                        "name": "eth0"
                    }
                ],
                "assigned_networks": [],
                "type": "bond"
            })
            print "!!! BOND configuration", configuration
            print "!!! configuration[4]", configuration[4]
    """

    #j: net ids
    public_id = cluster.networks['public']
    management_id = cluster.networks['management']
    storage_id = cluster.networks['storage']
    private_id = cluster.networks.get('private', None)  # there is no private net in GRE but in VLAN
    fixed_id = cluster.networks.get('fixed', None)  # there is no private net in GRE but in VLAN
    fuelweb_admin_id = cluster.networks['fuelweb_admin']

    net_provider = cluster_desc.get('net_provider')  # get the cluster type: VLAN, GRE or Nova
    net_segment_type = cluster_desc.get('net_segment_type')  # get the cluster type: VLAN, GRE or Nova')  # get the cluster type: VLAN, GRE or Nova

    print " net_provider is: ", net_provider

    #fuel release:
    fuel_info = cluster.get_releases()
    fuel_ver = fuel_info[0]["version"][-3:]
    print " fuel version: ", fuel_ver
    #print " version only: ", fuel_ver[-3:]

    #for every compute change eth0-eth4 settings and ADD bond0 interface
    ##if "compute" in node_desc['roles']:
    if "compute" or "controller" in node_desc['roles']:  # now bonding is on all nodes, incl controller
        #print " Compute detected:", node.id
        print  node_desc['roles'], "detected"#. node_id
        #j: change settings for compute
        for item in configuration:
            if item['name'] == 'eth0':
                item['assigned_networks'] = []

            if item['name'] == 'eth1':
                item['assigned_networks'] = []

            if item['name'] == 'eth2':
                item['assigned_networks'] = [
                    {
                        "id": fuelweb_admin_id,
                        "name": "fuelweb_admin"
                    }
                        ]

        #now append the bonding to eth0 and eth1:

        # Case#1: VLAN 6.0, VAL
        if fuel_ver == "6.0":
            #if net_provider == "neutron_vlan":
            if net_segment_type == "vlan":
                print "Configuring bond0 as a Fuel6.0, VLAN"
                configuration.append({
                        "name": "ovs-bond0",
                        "state": 'null',
                        "mac": 'null',
                        "mode": "lacp-balance-tcp",
                        "slaves": [
                            {
                                "name": "eth1"
                            },
                            {
                                "name": "eth0"
                            }
                        ],
                        "assigned_networks": [
                            {
                                "id": private_id,
                                "name": "private"
                            },
                                                {
                                "id": public_id,
                                "name": "public"
                            },
                            {
                                "id": management_id,
                                "name": "management"
                            },
                            {
                                "id": storage_id,
                                "name": "storage"
                            }

                        ],
                        "type": "bond"
                })
            #if net_provider == "neutron":
            if net_segment_type == "gre":
                print "Configuring bond0 as a Fuel6.0, GRE"
                configuration.append({
                        "name": "ovs-bond0",
                        "state": 'null',
                        "mac": 'null',
                        "mode": "lacp-balance-tcp",
                        "slaves": [
                            {
                                "name": "eth1"
                            },
                            {
                                "name": "eth0"
                            }
                        ],
                        "assigned_networks": [
                            {
                                "id": public_id,
                                "name": "public"
                            },
                            {
                                "id": management_id,
                                "name": "management"
                            },
                            {
                                "id": storage_id,
                                "name": "storage"
                            }

                        ],
                        "type": "bond"
                })

        # Case#1: VLAN 6.1
        if fuel_ver == "6.1":
        #if True:
            #Case 1.1: VLAN
            #if net_provider == "neutron_vlan":
            if net_segment_type == "vlan":
                print "Configuring bond0 as a Fuel6.1, VLAN"
                configuration.append({
                "name": "bond0",
                "state": "null",
                "assigned_networks": [
                            {
                                "id": private_id,
                                "name": "private"
                            },
                                                {
                                "id": public_id,
                                "name": "public"
                            },
                            {
                                "id": management_id,
                                "name": "management"
                            },
                            {
                                "id": storage_id,
                                "name": "storage"
                            }
                ],
                "bond_properties": {
                    "lacp_rate": "slow",
                    "type__": "linux",
                    "mode": "802.3ad",
                    "xmit_hash_policy": "layer2"
                },
                "mac": "null",
                "mode": "802.3ad",
                "slaves": [
                    {
                        "name": "eth1"
                    },
                    {
                        "name": "eth0"
                    }
                ],
                "type": "bond"
            })
            #Case 1.2: GRE
            #if net_provider == "neutron":
            if net_segment_type == "gre":
                print "Configuring bond0 as a Fuel6.1, GRE"
                configuration.append({
                "name": "bond0",
                "state": "null",
                "assigned_networks": [
                            {
                                "id": public_id,
                                "name": "public"
                            },
                            {
                                "id": management_id,
                                "name": "management"
                            },
                            {
                                "id": storage_id,
                                "name": "storage"
                            }
                ],
                "bond_properties": {
                    "lacp_rate": "slow",
                    "type__": "linux",
                    "mode": "802.3ad",
                    "xmit_hash_policy": "layer2"
                },
                "mac": "null",
                "mode": "802.3ad",
                "slaves": [
                    {
                        "name": "eth1"
                    },
                    {
                        "name": "eth0"
                    }
                ],
                "type": "bond"
            })

        # Case#1.3: Nova
            if net_provider == "nova_network":
                print "Configuring bond0 as a Fuel6.1, GRE"
                configuration.append({
                "name": "bond0",
                "state": "null",
                "assigned_networks": [
                            {
                                "id": public_id,
                                "name": "public"
                            },
                            {
                                "id": management_id,
                                "name": "management"
                            },
                            {
                                "id": storage_id,
                                "name": "storage"
                            },
                                                        {
                                "id": fixed_id,
                                "name": "fixed"
                            }
                ],
                "bond_properties": {
                    "lacp_rate": "slow",
                    "type__": "linux",
                    "mode": "802.3ad",
                    "xmit_hash_policy": "layer2"
                },
                "mac": "null",
                "mode": "802.3ad",
                "slaves": [
                    {
                        "name": "eth1"
                    },
                    {
                        "name": "eth0"
                    }
                ],
                "type": "bond"
            })

        ###########################
        ######   Fuel 7.0 #########
        ###########################

        # Case#3: VLAN 7.0
        if fuel_ver == "7.0":
        #if True:
            #Case 1.1: VLAN
            #if net_provider == "neutron_vlan":
            if net_segment_type == "vlan":
                print "Configuring bond0 as a Fuel7.0, VLAN"
                configuration.append({
                "name": "bond0",
                "state": "null",
                "assigned_networks": [
                            {
                                "id": private_id,
                                "name": "private"
                            },
                                                {
                                "id": public_id,
                                "name": "public"
                            },
                            {
                                "id": management_id,
                                "name": "management"
                            },
                            {
                                "id": storage_id,
                                "name": "storage"
                            }
                ],
                "bond_properties": {
                    "lacp_rate": "slow",
                    "type__": "linux",
                    "mode": "802.3ad",
                    "xmit_hash_policy": "layer2"
                },
                "mac": "null",
                "mode": "802.3ad",
                "slaves": [
                    {
                        "name": "eth1"
                    },
                    {
                        "name": "eth0"
                    }
                ],
                "type": "bond"
            })

            #Case 3.2: GRE
            if net_segment_type == "tun":
                print "Configuring bond0 as a Fuel7.0, GRE"
                configuration.append({
                "name": "bond0",
                "state": "null",
                "assigned_networks": [
                            {
                                "id": public_id,
                                "name": "public"
                            },
                            {
                                "id": management_id,
                                "name": "management"
                            },
                            {
                                "id": storage_id,
                                "name": "storage"
                            }
                ],
                "bond_properties": {
                    "lacp_rate": "slow",
                    "type__": "linux",
                    "mode": "802.3ad",
                    "xmit_hash_policy": "layer2"
                },
                "mac": "null",
                "mode": "802.3ad",
                "slaves": [
                    {
                        "name": "eth1"
                    },
                    {
                        "name": "eth0"
                    }
                ],
                "type": "bond"
            })






        ########## End of Fue;l 7.0 ###########



    if "controller" in node_desc['roles']:
        print " Controller detected: id", node.id


    print " Interfaces new configuration: ", configuration
    ##cluster.configure_interfaces(configuration, id = node.id)  # temp comment
    #cluster.prest.PUT('api/nodes/' + node.id + '/interfaces')
'''

def configure_disks(cluster, node_desc, node):
    configuration = cluster.get_disks(id=node.id)
    templ = "Disk info: {}"
    logger.info(templ.format(configuration))

    if 'disks' not in node_desc:
        logger.info("Target disk config is empty. Nothing to configure.")
        return

    target_disks = node_desc['disks']
    templ = "Target disk configuration: {}"
    logger.info(templ.format(target_disks))

    for disk in configuration:
        if disk['name'] in target_disks:
            templ = "Disk configuration before: {}"
            logger.info(templ.format(disk))
            target_volumes = target_disks[disk['name']]
            for volume in disk['volumes']:
                if target_volumes != None and volume['name'] in target_volumes:
                    volume['size'] = target_volumes[volume['name']]
                else:
                    volume['size'] = 0
            templ = "Disk configuration after: {}"
            logger.info(templ.format(disk))


    cluster.update_disks(configuration, id=node.id)


def create_cluster(conn, cluster):
    nodes_iter = match_nodes(conn, cluster['nodes'])

    #j:
    #return # temporary: to let cluster assembly to fail fast

    use_ceph = False

    if 'nodes' in cluster:
        for node in cluster['nodes']:
            if cluster['nodes'][node]['role'] == 'ceph-osd':
                use_ceph = True
                break

    if cluster.get('storage_type', None) == 'ceph':
        use_ceph = True

    if use_ceph:
        logger.info("Storage type: ceph")
    else:
        logger.info("Storage type: cinder")

    logger.info("Creating empty cluster")

    cluster_obj = create_empty_cluster(conn, cluster,
                                       use_ceph=use_ceph)

    try:
        if 'network' in cluster:
            logger.info("Setting network parameters")
            set_networks_params(cluster_obj, cluster['network'])


        for node_description, node in nodes_iter:
            #j:
            logger.debug(" node_desc: ", node_description, node)
            node.set_node_name(node_description['name'])
            templ = "Adding node {} with role {}"

            logger.info(templ.format(node.name, node_description['role']))

            cluster_obj.add_node(node, [node_description['role']])

            set_interface_params(cluster_obj, node_description, node)
            configure_disks(cluster_obj, node_description, node)


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
                        dest='fuelurl', required=True, default="http://172.16.52.112:8000")

    parser.add_argument('config_file',
                        help='yaml configuration file',
                        metavar="CLUSTER_CONFIG",
                        default="config.yaml")

    parser.add_argument('-d', '--debug',
                        help='allow debug logging',
                        action="store_true")

    parser.add_argument('-n', '--no-deploy',
                        help='doesn\'t deploy cluster',
                        action="store_true",
                        dest='nodeploy')

    return parser.parse_args(argv[1:])


def main(argv=None):
    if argv is None:
        argv = sys.argv

    args = parse_command_line(argv)

    ch = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    ch.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)

    log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    formatter = logging.Formatter(log_format, "%H:%M:%S")
    ch.setFormatter(formatter)

    logger.addHandler(ch)

#    fh = logging.FileHandler("log.txt")
#    fh.setLevel()

    conn = login(args.fuelurl, args.auth)
    cluster = yaml.load(open(args.config_file).read())

    fuel = FuelInfo(conn)

    for cluster_obj in fuel.clusters:
        logger.debug("cluster['name']:", cluster['name'])
        if cluster_obj.name == cluster['name']:
            cluster_obj.delete()
            wd = with_timeout(60, "Wait cluster deleted")
            wd(lambda co: not co.check_exists())(cluster_obj)

    c = create_cluster(conn, cluster)

    if args.nodeploy == False:
        c.start_deploy()
        #j:
        #c.wait_operational(60*60*60*1000) # original
        c.wait_operational(60*60*60*3000)

    return 0
'''
    nodes = [node for node in c.nodes]

    for i in range(len(nodes)):
        node_desc = cluster["disks"][i]
        node = nodes[i]
        value = node_desc.values()[0]

        if value == "n/a":
            pass
        else:
            print value
            node.update_disk_volumes(value)
'''


if __name__ == "__main__":
    exit(main(sys.argv))
