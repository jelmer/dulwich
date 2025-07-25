# test_paramiko_vendor.py
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

"""Tests for paramiko_vendor."""

import os
import socket
import tempfile
import threading
import time
from io import StringIO
from typing import Optional
from unittest import skipIf
from unittest.mock import patch

from .. import TestCase

try:
    import paramiko
except ImportError:
    has_paramiko = False
else:
    has_paramiko = True
    import paramiko.transport

    from dulwich.contrib.paramiko_vendor import ParamikoSSHVendor

    class Server(paramiko.ServerInterface):
        """http://docs.paramiko.org/en/2.4/api/server.html."""

        def __init__(self, commands, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.commands = commands

        def check_channel_exec_request(self, channel, command) -> bool:
            self.commands.append(command)
            return True

        def check_auth_password(self, username, password):
            if username == USER and password == PASSWORD:
                return paramiko.AUTH_SUCCESSFUL
            return paramiko.AUTH_FAILED

        def check_auth_publickey(self, username, key):
            pubkey = paramiko.RSAKey.from_private_key(StringIO(CLIENT_KEY))
            if username == USER and key == pubkey:
                return paramiko.AUTH_SUCCESSFUL
            return paramiko.AUTH_FAILED

        def check_channel_request(self, kind, chanid):
            if kind == "session":
                return paramiko.OPEN_SUCCEEDED
            return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

        def get_allowed_auths(self, username) -> str:
            return "password,publickey"

    class SSHServer:
        """A real SSH server using Paramiko that listens on a TCP port."""

        def __init__(self):
            self.commands = []
            self.server_socket = None
            self.server_thread = None
            self.host_key = paramiko.RSAKey.from_private_key(StringIO(SERVER_KEY))
            self.running = False
            self.connection_threads = []

        def start(self):
            """Start the SSH server on a random port."""
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(("127.0.0.1", 0))
            self.port = self.server_socket.getsockname()[1]
            self.server_socket.listen(5)

            self.running = True
            self.server_thread = threading.Thread(target=self._run_server)
            self.server_thread.daemon = True
            self.server_thread.start()

            # Give the server a moment to start up
            time.sleep(0.1)

        def stop(self):
            """Stop the SSH server."""
            self.running = False
            if self.server_socket:
                try:
                    self.server_socket.close()
                except OSError:
                    pass

            # Clean up connection threads
            for thread in self.connection_threads:
                if thread.is_alive():
                    thread.join(timeout=1)

            if self.server_thread and self.server_thread.is_alive():
                self.server_thread.join(timeout=5)

        def _run_server(self):
            """Main server loop."""
            self.server_socket.settimeout(
                1.0
            )  # Allow checking self.running periodically

            while self.running:
                try:
                    client_socket, addr = self.server_socket.accept()

                    # Handle each connection in a separate thread
                    conn_thread = threading.Thread(
                        target=self._handle_connection, args=(client_socket,)
                    )
                    conn_thread.daemon = True
                    conn_thread.start()
                    self.connection_threads.append(conn_thread)

                except socket.timeout:
                    # Normal timeout, continue to check if we should keep running
                    continue
                except OSError as e:
                    # Socket was closed, exit gracefully
                    if not self.running:
                        break
                    # Otherwise re-raise the error
                    raise e

        def _handle_connection(self, client_socket):
            """Handle a single SSH connection."""
            transport = None
            try:
                transport = paramiko.Transport(client_socket)
                transport.add_server_key(self.host_key)

                server = Server(self.commands)
                transport.start_server(server=server)

                # Wait for channel requests and handle them
                while self.running and transport.is_active():
                    channel = transport.accept(1)
                    if channel is None:
                        continue

                    # Handle channel in a separate thread to allow multiple channels
                    channel_thread = threading.Thread(
                        target=self._handle_channel, args=(channel,)
                    )
                    channel_thread.daemon = True
                    channel_thread.start()

            except paramiko.SSHException as e:
                print(f"SSH error in connection handler: {e}")
            except OSError as e:
                print(f"Socket error in connection handler: {e}")
            finally:
                if transport:
                    transport.close()
                if client_socket:
                    client_socket.close()

        def _handle_channel(self, channel):
            """Handle a single SSH channel - echo server."""
            try:
                # Set channel to blocking mode
                channel.setblocking(True)
                channel.settimeout(10.0)

                # Read all available data and echo it back
                while True:
                    try:
                        data = channel.recv(4096)
                        if not data:
                            break
                        # Echo the data back immediately
                        channel.send(data)
                    except socket.timeout:
                        # No more data available, break
                        break

            except paramiko.SSHException as e:
                print(f"SSH error in channel handler: {e}")
            except OSError as e:
                print(f"Socket error in channel handler: {e}")
            finally:
                try:
                    channel.close()
                except OSError:
                    pass


USER = "testuser"
PASSWORD = "test"
SERVER_KEY = """\
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAy/L1sSYAzxsMprtNXW4u/1jGXXkQmQ2xtmKVlR+RlIL3a1BH
bzTpPlZyjltAAwzIP8XRh0iJFKz5y3zSQChhX47ZGN0NvQsVct8R+YwsUonwfAJ+
JN0KBKKvC8fPHlzqBr3gX+ZxqsFH934tQ6wdQPH5eQWtdM8L826lMsH1737uyTGk
+mCSDjL3c6EzY83g7qhkJU2R4qbi6ne01FaWADzG8sOzXnHT+xpxtk8TTT8yCVUY
MmBNsSoA/ka3iWz70ghB+6Xb0WpFJZXWq1oYovviPAfZGZSrxBZMxsWMye70SdLl
TqsBEt0+miIcm9s0fvjWvQuhaHX6mZs5VO4r5QIDAQABAoIBAGYqeYWaYgFdrYLA
hUrubUCg+g3NHdFuGL4iuIgRXl4lFUh+2KoOuWDu8Uf60iA1AQNhV0sLvQ/Mbv3O
s4xMLisuZfaclctDiCUZNenqnDFkxEF7BjH1QJV94W5nU4wEQ3/JEmM4D2zYkfKb
FJW33JeyH6TOgUvohDYYEU1R+J9V8qA243p+ui1uVtNI6Pb0TXJnG5y9Ny4vkSWH
Fi0QoMPR1r9xJ4SEearGzA/crb4SmmDTKhGSoMsT3d5ATieLmwcS66xWz8w4oFGJ
yzDq24s4Fp9ccNjMf/xR8XRiekJv835gjEqwF9IXyvgOaq6XJ1iCqGPFDKa25nui
JnEstOkCgYEA/ZXk7aIanvdeJlTqpX578sJfCnrXLydzE8emk1b7+5mrzGxQ4/pM
PBQs2f8glT3t0O0mRX9NoRqnwrid88/b+cY4NCOICFZeasX336/gYQxyVeRLJS6Z
hnGEQqry8qS7PdKAyeHMNmZFrUh4EiHiObymEfQS+mkRUObn0cGBTw8CgYEAzeQU
D2baec1DawjppKaRynAvWjp+9ry1lZx9unryKVRwjRjkEpw+b3/+hdaF1IvsVSce
cNj+6W2guZ2tyHuPhZ64/4SJVyE2hKDSKD4xTb2nVjsMeN0bLD2UWXC9mwbx8nWa
2tmtUZ7a/okQb2cSdosJinRewLNqXIsBXamT1csCgYEA0cXb2RCOQQ6U3dTFPx4A
3vMXuA2iUKmrsqMoEx6T2LBow/Sefdkik1iFOdipVYwjXP+w9zC2QR1Rxez/DR/X
8ymceNUjxPHdrSoTQQG29dFcC92MpDeGXQcuyA+uZjcLhbrLOzYEvsOfxBb87NMG
14hNQPDNekTMREafYo9WrtUCgYAREK54+FVzcwf7fymedA/xb4r9N4v+d3W1iNsC
8d3Qfyc1CrMct8aVB07ZWQaOr2pPRIbJY7L9NhD0UZVt4I/sy1MaGqonhqE2LP4+
R6legDG2e/50ph7yc8gwAaA1kUXMiuLi8Nfkw/3yyvmJwklNegi4aRzRbA2Mzhi2
4q9WMQKBgQCb0JNyxHG4pvLWCF/j0Sm1FfvrpnqSv5678n1j4GX7Ka/TubOK1Y4K
U+Oib7dKa/zQMWehVFNTayrsq6bKVZ6q7zG+IHiRLw4wjeAxREFH6WUjDrn9vl2l
D48DKbBuBwuVOJWyq3qbfgJXojscgNQklrsPdXVhDwOF0dYxP89HnA==
-----END RSA PRIVATE KEY-----"""
CLIENT_KEY = """\
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAxvREKSElPOm/0z/nPO+j5rk2tjdgGcGc7We1QZ6TRXYLu7nN
GeEFIL4p8N1i6dmB+Eydt7xqCU79MWD6Yy4prFe1+/K1wCDUxIbFMxqQcX5zjJzd
i8j8PbcaUlVhP/OkjtkSxrXaGDO1BzfdV4iEBtTV/2l3zmLKJlt3jnOHLczP24CB
DTQKp3rKshbRefzot9Y+wnaK692RsYgsyo9YEP0GyWKG9topCHk13r46J6vGLeuj
ryUKqmbLJkzbJbIcEqwTDo5iHaCVqaMr5Hrb8BdMucSseqZQJsXSd+9tdRcIblUQ
38kZjmFMm4SFbruJcpZCNM2wNSZPIRX+3eiwNwIDAQABAoIBAHSacOBSJsr+jIi5
KUOTh9IPtzswVUiDKwARCjB9Sf8p4lKR4N1L/n9kNJyQhApeikgGT2GCMftmqgoo
tlculQoHFgemBlOmak0MV8NNzF5YKEy/GzF0CDH7gJfEpoyetVFrdA+2QS5yD6U9
XqKQxiBi2VEqdScmyyeT8AwzNYTnPeH/DOEcnbdRjqiy/CD79F49CQ1lX1Fuqm0K
I7BivBH1xo/rVnUP4F+IzocDqoga+Pjdj0LTXIgJlHQDSbhsQqWujWQDDuKb+MAw
sNK4Zf8ErV3j1PyA7f/M5LLq6zgstkW4qikDHo4SpZX8kFOO8tjqb7kujj7XqeaB
CxqrOTECgYEA73uWkrohcmDJ4KqbuL3tbExSCOUiaIV+sT1eGPNi7GCmXD4eW5Z4
75v2IHymW83lORSu/DrQ6sKr1nkuRpqr2iBzRmQpl/H+wahIhBXlnJ25uUjDsuPO
1Pq2LcmyD+jTxVnmbSe/q7O09gZQw3I6H4+BMHmpbf8tC97lqimzpJ0CgYEA1K0W
ZL70Xtn9quyHvbtae/BW07NZnxvUg4UaVIAL9Zu34JyplJzyzbIjrmlDbv6aRogH
/KtuG9tfbf55K/jjqNORiuRtzt1hUN1ye4dyW7tHx2/7lXdlqtyK40rQl8P0kqf8
zaS6BqjnobgSdSpg32rWoL/pcBHPdJCJEgQ8zeMCgYEA0/PK8TOhNIzrP1dgGSKn
hkkJ9etuB5nW5mEM7gJDFDf6JPupfJ/xiwe6z0fjKK9S57EhqgUYMB55XYnE5iIw
ZQ6BV9SAZ4V7VsRs4dJLdNC3tn/rDGHJBgCaym2PlbsX6rvFT+h1IC8dwv0V79Ui
Ehq9WTzkMoE8yhvNokvkPZUCgYEAgBAFxv5xGdh79ftdtXLmhnDvZ6S8l6Fjcxqo
Ay/jg66Tp43OU226iv/0mmZKM8Dd1xC8dnon4GBVc19jSYYiWBulrRPlx0Xo/o+K
CzZBN1lrXH1i6dqufpc0jq8TMf/N+q1q/c1uMupsKCY1/xVYpc+ok71b7J7c49zQ
nOeuUW8CgYA9Infooy65FTgbzca0c9kbCUBmcAPQ2ItH3JcPKWPQTDuV62HcT00o
fZdIV47Nez1W5Clk191RMy8TXuqI54kocciUWpThc6j44hz49oUueb8U4bLcEHzA
WxtWBWHwxfSmqgTXilEA3ALJp0kNolLnEttnhENwJpZHlqtes0ZA4w==
-----END RSA PRIVATE KEY-----"""


@skipIf(not has_paramiko, "paramiko is not installed")
class ParamikoSSHVendorTests(TestCase):
    def setUp(self) -> None:
        self.commands = []
        socket.setdefaulttimeout(10)
        self.addCleanup(socket.setdefaulttimeout, None)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen(5)
        self.addCleanup(self.socket.close)
        self.port = self.socket.getsockname()[1]
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def tearDown(self) -> None:
        self.thread.join()

    def _run(self) -> Optional[bool]:
        try:
            conn, addr = self.socket.accept()
        except OSError:
            return False
        self.transport = paramiko.Transport(conn)
        self.addCleanup(self.transport.close)
        host_key = paramiko.RSAKey.from_private_key(StringIO(SERVER_KEY))
        self.transport.add_server_key(host_key)
        server = Server(self.commands)
        self.transport.start_server(server=server)

    def test_run_command_password(self) -> None:
        vendor = ParamikoSSHVendor(
            allow_agent=False,
            look_for_keys=False,
        )
        vendor.run_command(
            "127.0.0.1",
            "test_run_command_password",
            username=USER,
            port=self.port,
            password=PASSWORD,
        )

        self.assertIn(b"test_run_command_password", self.commands)

    def test_run_command_with_privkey(self) -> None:
        key = paramiko.RSAKey.from_private_key(StringIO(CLIENT_KEY))

        vendor = ParamikoSSHVendor(
            allow_agent=False,
            look_for_keys=False,
        )
        vendor.run_command(
            "127.0.0.1",
            "test_run_command_with_privkey",
            username=USER,
            port=self.port,
            pkey=key,
        )

        self.assertIn(b"test_run_command_with_privkey", self.commands)

    def test_run_command_data_transfer(self) -> None:
        vendor = ParamikoSSHVendor(
            allow_agent=False,
            look_for_keys=False,
        )
        con = vendor.run_command(
            "127.0.0.1",
            "test_run_command_data_transfer",
            username=USER,
            port=self.port,
            password=PASSWORD,
        )

        self.assertIn(b"test_run_command_data_transfer", self.commands)

        channel = self.transport.accept(5)
        channel.send(b"stdout\n")
        channel.send_stderr(b"stderr\n")
        channel.close()

        # Fixme: it's return false
        # self.assertTrue(con.can_read())

        self.assertEqual(b"stdout\n", con.read(4096))

        # Fixme: it's return empty string
        # self.assertEqual(b'stderr\n', con.read_stderr(4096))

    def test_ssh_config_parsing(self) -> None:
        """Test that SSH config is properly parsed and used by ParamikoSSHVendor."""
        # Create a temporary SSH config file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".config", delete=False) as f:
            f.write(
                f"""
Host testserver
    HostName 127.0.0.1
    User testuser
    Port {self.port}
    IdentityFile /path/to/key
"""
            )
            config_path = f.name

        try:
            # Mock the config path
            with patch(
                "dulwich.contrib.paramiko_vendor.os.path.expanduser"
            ) as mock_expanduser:

                def side_effect(path):
                    if path == "~/.ssh/config":
                        return config_path
                    return path

                mock_expanduser.side_effect = side_effect

                vendor = ParamikoSSHVendor(
                    allow_agent=False,
                    look_for_keys=False,
                )

                # Test that SSH config values are loaded
                host_config = vendor.ssh_config.lookup("testserver")
                self.assertEqual(host_config["hostname"], "127.0.0.1")
                self.assertEqual(host_config["user"], "testuser")
                self.assertEqual(host_config["port"], str(self.port))
                self.assertIn("/path/to/key", host_config["identityfile"])

        finally:
            os.unlink(config_path)


@skipIf(not has_paramiko, "paramiko is not installed")
class ParamikoSSHVendorRealServerTests(TestCase):
    """Tests for ParamikoSSHVendor using a real SSH server listening on TCP."""

    def setUp(self) -> None:
        self.ssh_server = SSHServer()
        self.ssh_server.start()
        socket.setdefaulttimeout(10)
        self.addCleanup(socket.setdefaulttimeout, None)
        self.addCleanup(self.ssh_server.stop)

    def _run_command(self, command, **kwargs):
        """Helper to run a command with default vendor settings."""
        vendor = ParamikoSSHVendor(allow_agent=False, look_for_keys=False)
        kwargs.setdefault("port", self.ssh_server.port)
        kwargs.setdefault("username", USER)
        return vendor.run_command("127.0.0.1", command, **kwargs)

    def _test_echo(self, con, data):
        """Helper to test echo functionality."""
        con.write(data)
        response = con.read(len(data))
        self.assertEqual(data, response)

    def test_password_authentication_success(self) -> None:
        """Test successful password authentication."""
        con = self._run_command("test_password_auth", password=PASSWORD)
        self.assertIn(b"test_password_auth", self.ssh_server.commands)
        self._test_echo(con, b"hello\n")
        con.close()

    def test_key_authentication_success(self) -> None:
        """Test successful key authentication."""
        key = paramiko.RSAKey.from_private_key(StringIO(CLIENT_KEY))
        con = self._run_command("test_key_auth", pkey=key)
        self.assertIn(b"test_key_auth", self.ssh_server.commands)
        self._test_echo(con, b"key_test\n")
        con.close()

    def test_authentication_failures(self) -> None:
        """Test authentication failures."""
        # Wrong password
        with self.assertRaises(paramiko.AuthenticationException):
            self._run_command("should_fail", password="wrong_password")

        # Wrong key
        wrong_key = paramiko.RSAKey.generate(2048)
        with self.assertRaises(paramiko.AuthenticationException):
            self._run_command("should_fail", pkey=wrong_key)

    def test_connection_errors(self) -> None:
        """Test various connection errors."""
        vendor = ParamikoSSHVendor(allow_agent=False, look_for_keys=False)

        # Non-existent port
        with self.assertRaises((OSError, ConnectionRefusedError)):
            vendor.run_command(
                "127.0.0.1", "fail", username=USER, port=65432, password=PASSWORD
            )

        # Invalid hostname
        with self.assertRaises((socket.gaierror, OSError)):
            vendor.run_command(
                "invalid.hostname.example.com", "fail", username=USER, password=PASSWORD
            )

    def test_data_transfer(self) -> None:
        """Test various data transfer scenarios."""
        con = self._run_command("test_data", password=PASSWORD)

        # Large data (10KB)
        large_data = b"X" * 10240
        self._test_echo(con, large_data)

        # Binary data with all byte values
        binary_data = bytes(range(256))
        self._test_echo(con, binary_data)

        con.close()

    def test_multiple_connections(self) -> None:
        """Test multiple sequential connections."""
        # First connection
        con1 = self._run_command("test_connection_1", password=PASSWORD)
        self._test_echo(con1, b"first\n")
        con1.close()

        # Second connection
        con2 = self._run_command("test_connection_2", password=PASSWORD)
        self._test_echo(con2, b"second\n")
        con2.close()

        # Verify both commands were recorded
        self.assertIn(b"test_connection_1", self.ssh_server.commands)
        self.assertIn(b"test_connection_2", self.ssh_server.commands)

    def test_key_from_file(self) -> None:
        """Test authentication using key file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".key", delete=False) as f:
            f.write(CLIENT_KEY)
            key_path = f.name

        try:
            con = self._run_command("test_key_from_file", key_filename=key_path)
            self.assertIn(b"test_key_from_file", self.ssh_server.commands)
            self._test_echo(con, b"file_key_test\n")
            con.close()
        finally:
            os.unlink(key_path)

    def test_protocol_versions(self) -> None:
        """Test protocol version handling."""
        # Protocol version 2 (default)
        con = self._run_command(
            "test_protocol_v2", password=PASSWORD, protocol_version=2
        )
        self.assertIn(b"test_protocol_v2", self.ssh_server.commands)
        con.close()

        # Protocol version 1
        con = self._run_command(
            "test_protocol_v1", password=PASSWORD, protocol_version=1
        )
        self.assertIn(b"test_protocol_v1", self.ssh_server.commands)
        con.close()

    def test_vendor_options(self) -> None:
        """Test vendor initialization options."""
        # Test with timeout
        vendor = ParamikoSSHVendor(allow_agent=False, look_for_keys=False, timeout=1)
        con = vendor.run_command(
            "127.0.0.1",
            "test_timeout",
            username=USER,
            port=self.ssh_server.port,
            password=PASSWORD,
        )
        self.assertIn(b"test_timeout", self.ssh_server.commands)
        con.close()

    def test_can_read(self) -> None:
        """Test can_read functionality."""
        con = self._run_command("test_can_read", password=PASSWORD)

        # Check can_read returns bool
        self.assertIsInstance(con.can_read(), bool)

        # Send data and verify echo
        self._test_echo(con, b"test_data\n")
        con.close()

    def test_partial_reads(self) -> None:
        """Test reading data in small chunks."""
        con = self._run_command("test_partial", password=PASSWORD)

        test_data = b"0123456789" * 10  # 100 bytes
        con.write(test_data)

        # Read in 10-byte chunks
        received_data = b""
        while len(received_data) < len(test_data):
            chunk = con.read(10)
            if not chunk:
                break
            received_data += chunk

        self.assertEqual(test_data, received_data)
        con.close()

    def test_ssh_config_integration(self) -> None:
        """Test SSH config integration."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".config", delete=False) as f:
            f.write(f"""
Host testserver
    HostName 127.0.0.1
    User {USER}
    Port {self.ssh_server.port}
""")
            config_path = f.name

        try:
            with patch(
                "dulwich.contrib.paramiko_vendor.os.path.expanduser"
            ) as mock_expanduser:
                mock_expanduser.side_effect = (
                    lambda p: config_path if p == "~/.ssh/config" else p
                )

                vendor = ParamikoSSHVendor(allow_agent=False, look_for_keys=False)
                con = vendor.run_command(
                    "testserver", "test_ssh_config", password=PASSWORD
                )

                self.assertIn(b"test_ssh_config", self.ssh_server.commands)
                self._test_echo(con, b"config_test\n")
                con.close()
        finally:
            os.unlink(config_path)
