# paramiko_vendor.py -- paramiko implementation of the SSHVendor interface
# Copyright (C) 2013 Aaron O'Mullan <aaron.omullan@friendco.de>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as published by the Free Software Foundation; version 2.0
# or (at your option) any later version. You can redistribute it and/or
# modify it under the terms of either of these two licenses.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# You should have received a copy of the licenses; if not, see
# <http://www.gnu.org/licenses/> for a copy of the GNU General Public License
# and <http://www.apache.org/licenses/LICENSE-2.0> for a copy of the Apache
# License, Version 2.0.
#

"""Paramiko SSH support for Dulwich.

To use this implementation as the SSH implementation in Dulwich, override
the dulwich.client.get_ssh_vendor attribute:

  >>> from dulwich import client as _mod_client
  >>> from dulwich.contrib.paramiko_vendor import ParamikoSSHVendor
  >>> _mod_client.get_ssh_vendor = ParamikoSSHVendor

This implementation has comprehensive tests in tests/contrib/test_paramiko_vendor.py.
"""

import os
import warnings
from typing import Any, BinaryIO, Optional, cast

import paramiko
import paramiko.client
import paramiko.config


class _ParamikoWrapper:
    def __init__(self, client: paramiko.SSHClient, channel: paramiko.Channel) -> None:
        self.client = client
        self.channel = channel

        # Channel must block
        self.channel.setblocking(True)

    @property
    def stderr(self) -> BinaryIO:
        return cast(BinaryIO, self.channel.makefile_stderr("rb"))

    def can_read(self) -> bool:
        return self.channel.recv_ready()

    def write(self, data: bytes) -> None:
        return self.channel.sendall(data)

    def read(self, n: Optional[int] = None) -> bytes:
        data = self.channel.recv(n or 4096)
        data_len = len(data)

        # Closed socket
        if not data:
            return b""

        # Read more if needed
        if n and data_len < n:
            diff_len = n - data_len
            return data + self.read(diff_len)
        return data

    def close(self) -> None:
        self.channel.close()


class ParamikoSSHVendor:
    # http://docs.paramiko.org/en/2.4/api/client.html

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.ssh_config = self._load_ssh_config()

    def _load_ssh_config(self) -> paramiko.config.SSHConfig:
        """Load SSH configuration from ~/.ssh/config."""
        ssh_config = paramiko.config.SSHConfig()
        config_path = os.path.expanduser("~/.ssh/config")
        try:
            with open(config_path) as config_file:
                ssh_config.parse(config_file)
        except FileNotFoundError:
            # Config file doesn't exist - this is normal, ignore silently
            pass
        except (OSError, PermissionError) as e:
            # Config file exists but can't be read - warn user
            warnings.warn(f"Could not read SSH config file {config_path}: {e}")
        return ssh_config

    def run_command(
        self,
        host: str,
        command: str,
        username: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
        pkey: Optional[paramiko.PKey] = None,
        key_filename: Optional[str] = None,
        protocol_version: Optional[int] = None,
        **kwargs: object,
    ) -> _ParamikoWrapper:
        client = paramiko.SSHClient()

        # Get SSH config for this host
        host_config = self.ssh_config.lookup(host)

        connection_kwargs: dict[str, Any] = {
            "hostname": host_config.get("hostname", host)
        }
        connection_kwargs.update(self.kwargs)

        # Use SSH config values if not explicitly provided
        if username:
            connection_kwargs["username"] = username
        elif "user" in host_config:
            connection_kwargs["username"] = host_config["user"]

        if port:
            connection_kwargs["port"] = port
        elif "port" in host_config:
            connection_kwargs["port"] = int(host_config["port"])

        if password:
            connection_kwargs["password"] = password
        if pkey:
            connection_kwargs["pkey"] = pkey
        if key_filename:
            connection_kwargs["key_filename"] = key_filename
        elif "identityfile" in host_config:
            # Use the first identity file from SSH config
            identity_files = host_config["identityfile"]
            if isinstance(identity_files, list) and identity_files:
                connection_kwargs["key_filename"] = identity_files[0]
            elif isinstance(identity_files, str):
                connection_kwargs["key_filename"] = identity_files

        connection_kwargs.update(kwargs)

        policy = paramiko.client.MissingHostKeyPolicy()
        client.set_missing_host_key_policy(policy)
        client.connect(**connection_kwargs)

        # Open SSH session
        transport = client.get_transport()
        if transport is None:
            raise RuntimeError("Transport is None")
        channel = transport.open_session()

        if protocol_version is None or protocol_version == 2:
            channel.set_environment_variable(name="GIT_PROTOCOL", value="version=2")

        # Run commands
        channel.exec_command(command)

        return _ParamikoWrapper(client, channel)
