# paramiko_vendor.py -- paramiko implementation of the SSHVendor interface
# Copyright (C) 2013 Aaron O'Mullan <aaron.omullan@friendco.de>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# or (at your option) a later version of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

"""Paramiko SSH support for Dulwich.

To use this implementation as the SSH implementation in Dulwich, override
the dulwich.client.get_ssh_vendor attribute:

  >>> from dulwich import client as _mod_client
  >>> from dulwich.contrib.paramiko_vendor import ParamikoSSHVendor
  >>> _mod_client.get_ssh_vendor = ParamikoSSHVendor

This implementation is experimental and does not have any tests.
"""

import paramiko
import paramiko.client
import subprocess
import threading

class _ParamikoWrapper(object):
    STDERR_READ_N = 2048  # 2k

    def __init__(self, client, channel, progress_stderr=None):
        self.client = client
        self.channel = channel
        self.progress_stderr = progress_stderr
        self.should_monitor = bool(progress_stderr) or True
        self.monitor_thread = None
        self.stderr = b''

        # Channel must block
        self.channel.setblocking(True)

        # Start
        if self.should_monitor:
            self.monitor_thread = threading.Thread(
                target=self.monitor_stderr)
            self.monitor_thread.start()

    def monitor_stderr(self):
        while self.should_monitor:
            # Block and read
            data = self.read_stderr(self.STDERR_READ_N)

            # Socket closed
            if not data:
                self.should_monitor = False
                break

            # Emit data
            if self.progress_stderr:
                self.progress_stderr(data)

            # Append to buffer
            self.stderr += data

    def stop_monitoring(self):
        # Stop StdErr thread
        if self.should_monitor:
            self.should_monitor = False
            self.monitor_thread.join()

            # Get left over data
            data = self.channel.in_stderr_buffer.empty()
            self.stderr += data

    def can_read(self):
        return self.channel.recv_ready()

    def write(self, data):
        return self.channel.sendall(data)

    def read_stderr(self, n):
        return self.channel.recv_stderr(n)

    def read(self, n=None):
        data = self.channel.recv(n)
        data_len = len(data)

        # Closed socket
        if not data:
            return

        # Read more if needed
        if n and data_len < n:
            diff_len = n - data_len
            return data + self.read(diff_len)
        return data

    def close(self):
        self.channel.close()
        self.stop_monitoring()


class ParamikoSSHVendor(object):

    def __init__(self):
        self.ssh_kwargs = {}

    def run_command(self, host, command, username=None, port=None,
                    progress_stderr=None):
        if not isinstance(command, bytes):
            raise TypeError(command)
        # Paramiko needs an explicit port. None is not valid
        if port is None:
            port = 22

        client = paramiko.SSHClient()

        policy = paramiko.client.MissingHostKeyPolicy()
        client.set_missing_host_key_policy(policy)
        client.connect(host, username=username, port=port,
                       **self.ssh_kwargs)

        # Open SSH session
        channel = client.get_transport().open_session()

        # Run commands
        channel.exec_command(command)

        return _ParamikoWrapper(
            client, channel, progress_stderr=progress_stderr)
