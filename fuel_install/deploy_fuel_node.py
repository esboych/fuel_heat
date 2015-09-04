#!/usr/bin/env python

import time
import os.path
import shutil
import argparse
import tempfile
import ConfigParser
import subprocess

try:
    from netaddr import IPAddress, IPNetwork, valid_ipv4, valid_mac
    from pyinotify import WatchManager, Notifier, IN_CLOSE_NOWRITE
except ImportError as e:
    print '{}'.format(e)
    exit(1)

PXE_CONF_TEMPLATE = ("""DEFAULT fuel
PROMPT 0
TIMEOUT 0
TOTALTIMEOUT 0
ONTIMEOUT fuel

LABEL fuel
        KERNEL {image_mount_dir_relative}/isolinux/vmlinuz
        INITRD {image_mount_dir_relative}/isolinux/initrd.img
        IPAPPEND 2
        APPEND"""
" biosdevname=0 ks=nfs:{pxe_server_ip}:{ks_file_path} repo=nfs:{pxe_server_ip}:{image_mount_dir}"
" ip={fuel_ip} netmask={fuel_netmask} gw={fuel_gw} hostname={fuel_hostname} dns1=8.8.8.8"
" dns2=8.8.4.4 dhcp_interface=eth1 showmenu=no installdrive=sda ksdevice=bootif forceformat=yes"
" admin_ip=10.20.0.2 admin_netmask=255.255.255.0 admin_gw=10.20.0.1 admin_interface=eth0")


PXE_DEFAULT_CONF = """DEFAULT vesamenu.c32
PROMPT 0
TIMEOUT 100
TOTALTIMEOUT 3000
ONTIMEOUT BootHDD


MENU TITLE PXE Boot Menu

LABEL BootHDD
    MENU LABEL ^Boot from local HDD
    LOCALBOOT 0x0

LABEL BootHDD-2
    MENU LABEL Boot ^from local HDD-2
    LOCALBOOT 0x0
"""


class InstallationError(Exception):
    def __init__(self, message, mounted=False):
        super(InstallationError, self).__init__(message)
        self.mounted = mounted


def error(message):
    print message
    exit(1)


def write_file(file_name, content):
    try:
        with open(file_name, 'w') as f:
            f.write(content)
    except EnvironmentError as e:
        raise InstallationError('Cannot write file: {}'.format(e))


def wait_for_vm_boot(path, timeout=300):
    wait_for_vm_boot.ks_read = False

    def proc(_):
        wait_for_vm_boot.ks_read = True

    wm = WatchManager()
    notifier = Notifier(wm, timeout=1000)
    wm.add_watch(path, mask=IN_CLOSE_NOWRITE, rec=False, proc_fun=proc)
    start_time = int(time.time())

    notifier.process_events()
    while not wait_for_vm_boot.ks_read:
        if int(time.time()) - start_time > timeout:
            break
        if notifier.check_events():  #loop in case more events appear while we are processing
            notifier.read_events()
            notifier.process_events()
    else:
        return True
    print 'Timeout {} sec occured while waiting for Master Node reboot'.format(timeout)


def read_options():
    parser = argparse.ArgumentParser()
    parser.add_argument('config', help='Path to configuration file', type=argparse.FileType('r'))
    parser.add_argument('env', help='Environment name to process')
    subparsers = parser.add_subparsers(help='sub-command help', dest='command')
    parser_deploy = subparsers.add_parser('install-fuel', help='Install Fuel Master node')
    parser_deploy.add_argument('--iso', help='Path to Fuel ISO image', type=argparse.FileType('r'))
    subparsers.add_parser('reboot-node', help='Reboot Fuel Master node')
    return parser.parse_args()


def validate_config(options):
    config = ConfigParser.ConfigParser()
    config.readfp(options.config)
    if  not config.has_section(options.env):
        error('Environment {} does not exist in file {}'.format(options.env, options.config.name))
    env = dict(config.items(options.env))

    if not valid_mac(env['fuel_mac']):
        error('Wrong MAC address for Fuel node: {}'.format(env['fuel_mac']))

    for key in ['fuel_ip', 'fuel_netmask', 'fuel_gw', 'fuel_control_ip']:
        if not valid_ipv4(env[key]):
            error('Wrong IP address ({}) for {} parameter'.format(env[key], key))

    ip = IPNetwork(env['fuel_ip'] + '/' + env['fuel_netmask'])
    gw = IPAddress(env['fuel_gw'])
    if gw not in ip:
        error('Gateway address is not within network range')

    for path in [env['image_mount_dir'], env['ks_files_dir'],
                 env['ks_patch'], env['pxe_config_dir'], env['esxi_private_key_file']]:
        if not os.path.exists(path):
            error('Path {} does not exist or is not accessible'.format(path))

    env['pxe_config_file'] = os.path.join(
        env['pxe_config_dir'],
        '01-{}'.format(env['fuel_mac'].replace(':', '-')))
    return env


def create_temp_copy(path, temp_dir, prefix='', suffix=''):
    # Getting temp file name without actual file creation
    temp_name = prefix + next(tempfile._get_candidate_names()) + suffix
    temp_path = os.path.join(temp_dir, temp_name)
    shutil.copy2(path, temp_path)
    return temp_path


def install_fuel(env):
    mounted = False
    mount_dir = env['image_mount_dir']

    try:
        temp_dir = tempfile.mkdtemp(prefix='TEMP.', dir=mount_dir)
    except OSError:
        raise InstallationError('Cannot create temp directory under {} path'.format(mount_dir))

    env['image_mount_dir'] = temp_dir
    env['image_mount_dir_relative'] = os.path.relpath(temp_dir, env['tftp_root'])
    env['ks_file_path'] = os.path.join(temp_dir, env['ks_file'])

    retcode = subprocess.call(["mount", "-o", "ro,loop", ARGS.iso.name, env['image_mount_dir']])
    if retcode:
        raise InstallationError('Cannot mount image {} to directory {}'.format(
            ARGS.iso.name, env['image_mount_dir']))

    try:
        ks_backup = create_temp_copy(env['ks_file_path'],
                                     env['ks_files_dir'],
                                     prefix='ks_',
                                     suffix='.cfg')
        env['ks_file_path'] = ks_backup
    except IOError:
        raise InstallationError(
            mounted=mounted,
            message='Cannot copy {}. It is missing or destination {} is not accessible'.format(
                env['ks_file_path'], env['ks_files_dir']))

    retcode = subprocess.call(["patch", ks_backup, "-i", env['ks_patch']])
    if retcode:
        raise InstallationError('Cannot patch file {} with {}'.format(ks_backup, env['ks_patch']))

    # create pxe conf
    write_file(env['pxe_config_file'], PXE_CONF_TEMPLATE.format(**env))
    # reboot node
    vminfo = subprocess.check_output('ssh -i {} root@{} vim-cmd vmsvc/getallvms'.format(
        env['esxi_private_key_file'], env['fuel_control_ip']), shell=True)

    for line in vminfo.splitlines():
        if env['fuel_vm_name'] in line:
            vmid = line.strip().split()[0]
            print vmid
            break
    else:
        raise InstallationError('VM {} does not exist on ESXi host {}'.format(
            env['fuel_vm_name'], env['fuel_control_ip']), mounted=mounted)

    vmstate = subprocess.check_output('ssh -i {} root@{} vim-cmd vmsvc/power.getstate {}'.format(
        env['esxi_private_key_file'], env['fuel_control_ip'], vmid), shell=True).splitlines()[-1]
    print vmstate

    if ' on' in vmstate:
        command = 'reset'
    elif 'off' in vmstate:
        command = 'on'
    else:
        raise InstallationError('VM {} is in unknown power state: {}'.format(
            env['fuel_vm_name'], vmstate), mounted=mounted)
    subprocess.call('ssh -i {} root@{} vim-cmd vmsvc/power.{} {}'.format(
        env['esxi_private_key_file'], env['fuel_control_ip'], command, vmid), shell=True)

    # wait for next reboot
    wait_for_vm_boot(env['pxe_config_file'], timeout=300)
    # move pxe conf
    shutil.move(env['pxe_config_file'], env['pxe_config_file'] + '.bak')
    write_file(env['pxe_config_file'], PXE_DEFAULT_CONF)
    # wait for pxe conf access
    wait_for_vm_boot(env['pxe_config_file'], timeout=600)
    # unmount iso

    retcode = subprocess.call(["umount", "-l", env['image_mount_dir']])
    if retcode:
        raise InstallationError('Cannot unmount image {}'.format(ARGS.iso.name))

def reset_node(env):
    host = env['fuel_ip_']
    print host


if __name__ == '__main__':
    ARGS = read_options()
    ENV = validate_config(ARGS)

    CMD = ARGS.command.lower()
    if CMD == 'install-fuel':
        try:
            install_fuel(ENV)
        except InstallationError as e:
            if e.mounted:
                # unmount()
                pass
            error(e.message)
    elif CMD == 'reset-node':
        reset_node(ENV)
    else:
        error('Command {} is not supported.'.format(CMD))
