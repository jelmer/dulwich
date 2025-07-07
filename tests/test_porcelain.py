# test_porcelain.py -- porcelain tests
# Copyright (C) 2013 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
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

"""Tests for dulwich.porcelain."""

import contextlib
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from io import BytesIO, StringIO
from unittest import skipIf

from dulwich import porcelain
from dulwich.client import SendPackResult
from dulwich.diff_tree import tree_changes
from dulwich.errors import CommitError
from dulwich.object_store import DEFAULT_TEMPFILE_GRACE_PERIOD
from dulwich.objects import ZERO_SHA, Blob, Commit, Tag, Tree
from dulwich.porcelain import (
    CheckoutError,  # Hypothetical or real error class
    CountObjectsResult,
    add,
    commit,
)
from dulwich.repo import NoIndexPresent, Repo
from dulwich.server import DictBackend
from dulwich.tests.utils import build_commit_graph, make_commit, make_object
from dulwich.web import make_server, make_wsgi_chain

from . import TestCase

try:
    import gpg
except ImportError:
    gpg = None


def flat_walk_dir(dir_to_walk):
    for dirpath, _, filenames in os.walk(dir_to_walk):
        rel_dirpath = os.path.relpath(dirpath, dir_to_walk)
        if not dirpath == dir_to_walk:
            yield rel_dirpath
        for filename in filenames:
            if dirpath == dir_to_walk:
                yield filename
            else:
                yield os.path.join(rel_dirpath, filename)


class PorcelainTestCase(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)
        self.addCleanup(self.repo.close)

    def assertRecentTimestamp(self, ts) -> None:
        # On some slow CIs it does actually take more than 5 seconds to go from
        # creating the tag to here.
        self.assertLess(time.time() - ts, 50)


@skipIf(gpg is None, "gpg is not available")
class PorcelainGpgTestCase(PorcelainTestCase):
    DEFAULT_KEY = """
-----BEGIN PGP PRIVATE KEY BLOCK-----

lQVYBGBjIyIBDADAwydvMPQqeEiK54FG1DHwT5sQejAaJOb+PsOhVa4fLcKsrO3F
g5CxO+/9BHCXAr8xQAtp/gOhDN05fyK3MFyGlL9s+Cd8xf34S3R4rN/qbF0oZmaa
FW0MuGnniq54HINs8KshadVn1Dhi/GYSJ588qNFRl/qxFTYAk+zaGsgX/QgFfy0f
djWXJLypZXu9D6DlyJ0cPSzUlfBkI2Ytx6grzIquRjY0FbkjK3l+iGsQ+ebRMdcP
Sqd5iTN9XuzIUVoBFAZBRjibKV3N2wxlnCbfLlzCyDp7rktzSThzjJ2pVDuLrMAx
6/L9hIhwmFwdtY4FBFGvMR0b0Ugh3kCsRWr8sgj9I7dUoLHid6ObYhJFhnD3GzRc
U+xX1uy3iTCqJDsG334aQIhC5Giuxln4SUZna2MNbq65ksh38N1aM/t3+Dc/TKVB
rb5KWicRPCQ4DIQkHMDCSPyj+dvRLCPzIaPvHD7IrCfHYHOWuvvPGCpwjo0As3iP
IecoMeguPLVaqgcAEQEAAQAL/i5/pQaUd4G7LDydpbixPS6r9UrfPrU/y5zvBP/p
DCynPDutJ1oq539pZvXQ2VwEJJy7x0UVKkjyMndJLNWly9wHC7o8jkHx/NalVP47
LXR+GWbCdOOcYYbdAWcCNB3zOtzPnWhdAEagkc2G9xRQDIB0dLHLCIUpCbLP/CWM
qlHnDsVMrVTWjgzcpsnyGgw8NeLYJtYGB8dsN+XgCCjo7a9LEvUBKNgdmWBbf14/
iBw7PCugazFcH9QYfZwzhsi3nqRRagTXHbxFRG0LD9Ro9qCEutHYGP2PJ59Nj8+M
zaVkJj/OxWxVOGvn2q16mQBCjKpbWfqXZVVl+G5DGOmiSTZqXy+3j6JCKdOMy6Qd
JBHOHhFZXYmWYaaPzoc33T/C3QhMfY5sOtUDLJmV05Wi4dyBeNBEslYgUuTk/jXb
5ZAie25eDdrsoqkcnSs2ZguMF7AXhe6il2zVhUUMs/6UZgd6I7I4Is0HXT/pnxEp
uiTRFu4v8E+u+5a8O3pffe5boQYA3TsIxceen20qY+kRaTOkURHMZLn/y6KLW8bZ
rNJyXWS9hBAcbbSGhfOwYfzbDCM17yPQO3E2zo8lcGdRklUdIIaCxQwtu36N5dfx
OLCCQc5LmYdl/EAm91iAhrr7dNntZ18MU09gdzUu+ONZwu4CP3cJT83+qYZULso8
4Fvd/X8IEfGZ7kM+ylrdqBwtlrn8yYXtom+ows2M2UuNR53B+BUOd73kVLTkTCjE
JH63+nE8BqG7tDLCMws+23SAA3xxBgDfDrr0x7zCozQKVQEqBzQr9Uoo/c/ZjAfi
syzNSrDz+g5gqJYtuL9XpPJVWf6V1GXVyJlSbxR9CjTkBxmlPxpvV25IsbVSsh0o
aqkf2eWpbCL6Qb2E0jd1rvf8sGeTTohzYfiSVVsC2t9ngRO/CmetizwQBvRzLGMZ
4mtAPiy7ZEDc2dFrPp7zlKISYmJZUx/DJVuZWuOrVMpBP+bSgJXoMTlICxZUqUnE
2VKVStb/L+Tl8XCwIWdrZb9BaDnHqfcGAM2B4HNPxP88Yj1tEDly/vqeb3vVMhj+
S1lunnLdgxp46YyuTMYAzj88eCGurRtzBsdxxlGAsioEnZGebEqAHQbieKq/DO6I
MOMZHMSVBDqyyIx3assGlxSX8BSFW0lhKyT7i0XqnAgCJ9f/5oq0SbFGq+01VQb7
jIx9PbcYJORxsE0JG/CXXPv27bRtQXsudkWGSYvC0NLOgk4z8+kQpQtyFh16lujq
WRwMeriu0qNDjCa1/eHIKDovhAZ3GyO5/9m1tBlUZXN0IFVzZXIgPHRlc3RAdGVz
dC5jb20+iQHOBBMBCAA4AhsDBQsJCAcCBhUKCQgLAgQWAgMBAh4BAheAFiEEjrR8
MQ4fJK44PYMvfN2AClLmXiYFAmDcEZEACgkQfN2AClLmXibZzgv/ZfeTpTuqQE1W
C1jT5KpQExnt0BizTX0U7BvSn8Fr6VXTyol6kYc3u71GLUuJyawCLtIzOXqOXJvz
bjcZqymcMADuftKcfMy513FhbF6MhdVd6QoeBP6+7/xXOFJCi+QVYF7SQ2h7K1Qm
+yXOiAMgSxhCZQGPBNJLlDUOd47nSIMANvlumFtmLY/1FD7RpG7WQWjeX1mnxNTw
hUU+Yv7GuFc/JprXCIYqHbhWfvXyVtae2ZK4xuVi5eqwA2RfggOVM7drb+CgPhG0
+9aEDDLOZqVi65wK7J73Puo3rFTbPQMljxw5s27rWqF+vB6hhVdJOPNomWy3naPi
k5MW0mhsacASz1WYndpZz+XaQTq/wJF5HUyyeUWJ0vlOEdwx021PHcqSTyfNnkjD
KncrE21t2sxWRsgGDETxIwkd2b2HNGAvveUD0ffFK/oJHGSXjAERFGc3wuiDj3mQ
BvKm4wt4QF9ZMrCdhMAA6ax5kfEUqQR4ntmrJk/khp/mV7TILaI4nQVYBGBjIyIB
DADghIo9wXnRxzfdDTvwnP8dHpLAIaPokgdpyLswqUCixJWiW2xcV6weUjEWwH6n
eN/t1uZYVehbrotxVPla+MPvzhxp6/cmG+2lhzEBOp6zRwnL1wIB6HoKJfpREhyM
c8rLR0zMso1L1bJTyydvnu07a7BWo3VWKjilb0rEZZUSD/2hidx5HxMOJSoidLWe
d/PPuv6yht3NtA4UThlcfldm9G6PbqCdm1kMEKAkq0wVJvhPJ6gEFRNJimgygfUw
MDFXEIhQtxjgdV5Uoz3O5452VLoRsDlgpi3E0WDGj7WXDaO5uSU0T5aJgVgHCP/f
xZhHuQFk2YYIl5nCBpOZyWWI0IKmscTuEwzpkhICQDQFvcMZ5ibsl7wA2P7YTrQf
FDMjjzuaK80GYPfxDFlyKUyLqFt8w/QzsZLDLX7+jxIEpbRAaMw/JsWqm5BMxxbS
3CIQiS5S3oSKDsNINelqWFfwvLhvlQra8gIxyNTlek25OdgG66BiiX+seH8A/ql+
F+MAEQEAAQAL/1jrNSLjMt9pwo6qFKClVQZP2vf7+sH7v7LeHIDXr3EnYUnVYnOq
B1FU5PspTp/+J9W25DB9CZLx7Gj8qeslFdiuLSOoIBB4RCToB3kAoeTH0DHqW/Gs
hFTrmJkuDp9zpo/ek6SIXJx5rHAyR9KVw0fizQprH2f6PcgLbTWeM61dJuqowmg3
7eCOyIKv7VQvFqEhYokLD+JNmrvg+Htg0DXGvdjRjAwPf/NezEXpj67a6cHTp1/C
hwp7pevG+3fTxaCJFesl5/TxxtnaBLE8m2uo/S6Hxgn9l0edonroe1QlTjEqGLy2
7qi2z5Rem+v6GWNDRgvAWur13v8FNdyduHlioG/NgRsU9mE2MYeFsfi3cfNpJQp/
wC9PSCIXrb/45mkS8KyjZpCrIPB9RV/m0MREq01TPom7rstZc4A1pD0Ot7AtUYS3
e95zLyEmeLziPJ9fV4fgPmEudDr1uItnmV0LOskKlpg5sc0hhdrwYoobfkKt2dx6
DqfMlcM1ZkUbLQYA4jwfpFJG4HmYvjL2xCJxM0ycjvMbqFN+4UjgYWVlRfOrm1V4
Op86FjbRbV6OOCNhznotAg7mul4xtzrrTkK8o3YLBeJseDgl4AWuzXtNa9hE0XpK
9gJoEHUuBOOsamVh2HpXESFyE5CclOV7JSh541TlZKfnqfZYCg4JSbp0UijkawCL
5bJJUiGGMD9rZUxIAKQO1DvUEzptS7Jl6S3y5sbIIhilp4KfYWbSk3PPu9CnZD5b
LhEQp0elxnb/IL8PBgD+DpTeC8unkGKXUpbe9x0ISI6V1D6FmJq/FxNg7fMa3QCh
fGiAyoTm80ZETynj+blRaDO3gY4lTLa3Opubof1EqK2QmwXmpyvXEZNYcQfQ2CCS
GOWUCK8jEQamUPf1PWndZXJUmROI1WukhlL71V/ir6zQeVCv1wcwPwclJPnAe87u
pEklnCYpvsEldwHUX9u0BWzoULIEsi+ddtHmT0KTeF/DHRy0W15jIHbjFqhqckj1
/6fmr7l7kIi/kN4vWe0F/0Q8IXX+cVMgbl3aIuaGcvENLGcoAsAtPGx88SfRgmfu
HK64Y7hx1m+Bo215rxJzZRjqHTBPp0BmCi+JKkaavIBrYRbsx20gveI4dzhLcUhB
kiT4Q7oz0/VbGHS1CEf9KFeS/YOGj57s4yHauSVI0XdP9kBRTWmXvBkzsooB2cKH
hwhUN7iiT1k717CiTNUT6Q/pcPFCyNuMoBBGQTU206JEgIjQvI3f8xMUMGmGVVQz
9/k716ycnhb2JZ/Q/AyQIeHJiQG2BBgBCAAgAhsMFiEEjrR8MQ4fJK44PYMvfN2A
ClLmXiYFAmDcEa4ACgkQfN2AClLmXiZxxQv/XaMN0hPCygtrQMbCsTNb34JbvJzh
hngPuUAfTbRHrR3YeATyQofNbL0DD3fvfzeFF8qESqvzCSZxS6dYsXPd4MCJTzlp
zYBZ2X0sOrgDqZvqCZKN72RKgdk0KvthdzAxsIm2dfcQOxxowXMxhJEXZmsFpusx
jKJxOcrfVRjXJnh9isY0NpCoqMQ+3k3wDJ3VGEHV7G+A+vFkWfbLJF5huQ96uaH9
Uc+jUsREUH9G82ZBqpoioEN8Ith4VXpYnKdTMonK/+ZcyeraJZhXrvbjnEomKdzU
0pu4bt1HlLR3dcnpjN7b009MBf2xLgEfQk2nPZ4zzY+tDkxygtPllaB4dldFjBpT
j7Q+t49sWMjmlJUbLlHfuJ7nUUK5+cGjBsWVObAEcyfemHWCTVFnEa2BJslGC08X
rFcjRRcMEr9ct4551QFBHsv3O/Wp3/wqczYgE9itSnGT05w+4vLt4smG+dnEHjRJ
brMb2upTHa+kjktjdO96/BgSnKYqmNmPB/qB
=ivA/
-----END PGP PRIVATE KEY BLOCK-----
    """

    DEFAULT_KEY_ID = "8EB47C310E1F24AE383D832F7CDD800A52E65E26"

    NON_DEFAULT_KEY = """
-----BEGIN PGP PRIVATE KEY BLOCK-----

lQVYBGBjI0ABDADGWBRp+t02emfzUlhrc1psqIhhecFm6Em0Kv33cfDpnfoMF1tK
Yy/4eLYIR7FmpdbFPcDThFNHbXJzBi00L1mp0XQE2l50h/2bDAAgREdZ+NVo5a7/
RSZjauNU1PxW6pnXMehEh1tyIQmV78jAukaakwaicrpIenMiFUN3fAKHnLuFffA6
t0f3LqJvTDhUw/o2vPgw5e6UDQhA1C+KTv1KXVrhJNo88a3hZqCZ76z3drKR411Q
zYgT4DUb8lfnbN+z2wfqT9oM5cegh2k86/mxAA3BYOeQrhmQo/7uhezcgbxtdGZr
YlbuaNDTSBrn10ZoaxLPo2dJe2zWxgD6MpvsGU1w3tcRW508qo/+xoWp2/pDzmok
+uhOh1NAj9zB05VWBz1r7oBgCOIKpkD/LD4VKq59etsZ/UnrYDwKdXWZp7uhshkU
M7N35lUJcR76a852dlMdrgpmY18+BP7+o7M+5ElHTiqQbMuE1nHTg8RgVpdV+tUx
dg6GWY/XHf5asm8AEQEAAQAL/A85epOp+GnymmEQfI3+5D178D//Lwu9n86vECB6
xAHCqQtdjZnXpDp/1YUsL59P8nzgYRk7SoMskQDoQ/cB/XFuDOhEdMSgHaTVlnrj
ktCCq6rqGnUosyolbb64vIfVaSqd/5SnCStpAsnaBoBYrAu4ZmV4xfjDQWwn0q5s
u+r56mD0SkjPgbwk/b3qTVagVmf2OFzUgWwm1e/X+bA1oPag1NV8VS4hZPXswT4f
qhiyqUFOgP6vUBcqehkjkIDIl/54xII7/P5tp3LIZawvIXqHKNTqYPCqaCqCj+SL
vMYDIb6acjescfZoM71eAeHAANeFZzr/rwfBT+dEP6qKmPXNcvgE11X44ZCr04nT
zOV/uDUifEvKT5qgtyJpSFEVr7EXubJPKoNNhoYqq9z1pYU7IedX5BloiVXKOKTY
0pk7JkLqf3g5fYtXh/wol1owemITJy5V5PgaqZvk491LkI6S+kWC7ANYUg+TDPIW
afxW3E5N1CYV6XDAl0ZihbLcoQYAy0Ky/p/wayWKePyuPBLwx9O89GSONK2pQljZ
yaAgxPQ5/i1vx6LIMg7k/722bXR9W3zOjWOin4eatPM3d2hkG96HFvnBqXSmXOPV
03Xqy1/B5Tj8E9naLKUHE/OBQEc363DgLLG9db5HfPlpAngeppYPdyWkhzXyzkgS
PylaE5eW3zkdjEbYJ6RBTecTZEgBaMvJNPdWbn//frpP7kGvyiCg5Es+WjLInUZ6
0sdifcNTCewzLXK80v/y5mVOdJhPBgD5zs9cYdyiQJayqAuOr+He1eMHMVUbm9as
qBmPrst398eBW9ZYF7eBfTSlUf6B+WnvyLKEGsUf/7IK0EWDlzoBuWzWiHjUAY1g
m9eTV2MnvCCCefqCErWwfFo2nWOasAZA9sKD+ICIBY4tbtvSl4yfLBzTMwSvs9ZS
K1ocPSYUnhm2miSWZ8RLZPH7roHQasNHpyq/AX7DahFf2S/bJ+46ZGZ8Pigr7hA+
MjmpQ4qVdb5SaViPmZhAKO+PjuCHm+EF/2H0Y3Sl4eXgxZWoQVOUeXdWg9eMfYrj
XDtUMIFppV/QxbeztZKvJdfk64vt/crvLsOp0hOky9cKwY89r4QaHfexU3qR+qDq
UlMvR1rHk7dS5HZAtw0xKsFJNkuDxvBkMqv8Los8zp3nUl+U99dfZOArzNkW38wx
FPa0ixkC9za2BkDrWEA8vTnxw0A2upIFegDUhwOByrSyfPPnG3tKGeqt3Izb/kDk
Q9vmo+HgxBOguMIvlzbBfQZwtbd/gXzlvPqCtCJBbm90aGVyIFRlc3QgVXNlciA8
dGVzdDJAdGVzdC5jb20+iQHOBBMBCAA4AhsDBQsJCAcCBhUKCQgLAgQWAgMBAh4B
AheAFiEEapM5P1DF5qzT1vtFuTYhLttOFMAFAmDcEeEACgkQuTYhLttOFMDe0Qv/
Qx/bzXztJ3BCc+CYAVDx7Kr37S68etwwLgcWzhG+CDeMB5F/QE+upKgxy2iaqQFR
mxfOMgf/TIQkUfkbaASzK1LpnesYO85pk7XYjoN1bYEHiXTkeW+bgB6aJIxrRmO2
SrWasdBC/DsI3Mrya8YMt/TiHC6VpRJVxCe5vv7/kZC4CXrgTBnZocXx/YXimbke
poPMVdbvhYh6N0aGeS38jRKgyN10KXmhDTAQDwseVFavBWAjVfx3DEwjtK2Z2GbA
aL8JvAwRtqiPFkDMIKPL4UwxtXFws8SpMt6juroUkNyf6+BxNWYqmwXHPy8zCJAb
xkxIJMlEc+s7qQsP3fILOo8Xn+dVzJ5sa5AoARoXm1GMjsdqaKAzq99Dic/dHnaQ
Civev1PQsdwlYW2C2wNXNeIrxMndbDMFfNuZ6BnGHWJ/wjcp/pFs4YkyyZN8JH7L
hP2FO4Jgham3AuP13kC3Ivea7V6hR8QNcDZRwFPOMIX4tXwQv1T72+7DZGaA25O7
nQVXBGBjI0ABDADJMBYIcG0Yil9YxFs7aYzNbd7alUAr89VbY8eIGPHP3INFPM1w
lBQCu+4j6xdEbhMpppLBZ9A5TEylP4C6qLtPa+oLtPeuSw8gHDE10XE4lbgPs376
rL60XdImSOHhiduACUefYjqpcmFH9Bim1CC+koArYrSQJQx1Jri+OpnTaL/8UID0
KzD/kEgMVGlHIVj9oJmb4+j9pW8I/g0wDSnIaEKFMxqu6SIVJ1GWj+MUMvZigjLC
sNCZd7PnbOC5VeU3SsXj6he74Jx0AmGMPWIHi9M0DjHO5d1cCbXTnud8xxM1bOh4
7aCTnMK5cVyIr+adihgJpVVhrndSM8aklBPRgtozrGNCgF2CkYU2P1blxfloNr/8
UZpM83o+s1aObBszzRNLxnpNORqoLqjfPtLEPQnagxE+4EapCq0NZ/x6yO5VTwwp
NljdFAEk40uGuKyn1QA3uNMHy5DlpLl+tU7t1KEovdZ+OVYsYKZhVzw0MTpKogk9
JI7AN0q62ronPskAEQEAAQAL+O8BUSt1ZCVjPSIXIsrR+ZOSkszZwgJ1CWIoh0IH
YD2vmcMHGIhFYgBdgerpvhptKhaw7GcXDScEnYkyh5s4GE2hxclik1tbj/x1gYCN
8BNoyeDdPFxQG73qN12D99QYEctpOsz9xPLIDwmL0j1ehAfhwqHIAPm9Ca+i8JYM
x/F+35S/jnKDXRI+NVlwbiEyXKXxxIqNlpy9i8sDBGexO5H5Sg0zSN/B1duLekGD
biDw6gLc6bCgnS+0JOUpU07Z2fccMOY9ncjKGD2uIb/ePPUaek92GCQyq0eorCIV
brcQsRc5sSsNtnRKQTQtxioROeDg7kf2oWySeHTswlXW/219ihrSXgteHJd+rPm7
DYLEeGLRny8bRKv8rQdAtApHaJE4dAATXeY4RYo4NlXHYaztGYtU6kiM/3zCfWAe
9Nn+Wh9jMTZrjefUCagS5r6ZqAh7veNo/vgIGaCLh0a1Ypa0Yk9KFrn3LYEM3zgk
3m3bn+7qgy5cUYXoJ3DGJJEhBgDPonpW0WElqLs5ZMem1ha85SC38F0IkAaSuzuz
v3eORiKWuyJGF32Q2XHa1RHQs1JtUKd8rxFer3b8Oq71zLz6JtVc9dmRudvgcJYX
0PC11F6WGjZFSSp39dajFp0A5DKUs39F3w7J1yuDM56TDIN810ywufGAHARY1pZb
UJAy/dTqjFnCbNjpAakor3hVzqxcmUG+7Y2X9c2AGncT1MqAQC3M8JZcuZvkK8A9
cMk8B914ryYE7VsZMdMhyTwHmykGAPgNLLa3RDETeGeGCKWI+ZPOoU0ib5JtJZ1d
P3tNwfZKuZBZXKW9gqYqyBa/qhMip84SP30pr/TvulcdAFC759HK8sQZyJ6Vw24P
c+5ssRxrQUEw1rvJPWhmQCmCOZHBMQl5T6eaTOpR5u3aUKTMlxPKhK9eC1dCSTnI
/nyL8An3VKnLy+K/LI42YGphBVLLJmBewuTVDIJviWRdntiG8dElyEJMOywUltk3
2CEmqgsD9tPO8rXZjnMrMn3gfsiaoQYA6/6/e2utkHr7gAoWBgrBBdqVHsvqh5Ro
2DjLAOpZItO/EdCJfDAmbTYOa04535sBDP2tcH/vipPOPpbr1Y9Y/mNsKCulNxed
yqAmEkKOcerLUP5UHju0AB6VBjHJFdU2mqT+UjPyBk7WeKXgFomyoYMv3KpNOFWR
xi0Xji4kKHbttA6Hy3UcGPr9acyUAlDYeKmxbSUYIPhw32bbGrX9+F5YriTufRsG
3jftQVo9zqdcQSD/5pUTMn3EYbEcohYB2YWJAbYEGAEIACACGwwWIQRqkzk/UMXm
rNPW+0W5NiEu204UwAUCYNwR6wAKCRC5NiEu204UwOPnC/92PgB1c3h9FBXH1maz
g29fndHIHH65VLgqMiQ7HAMojwRlT5Xnj5tdkCBmszRkv5vMvdJRa3ZY8Ed/Inqr
hxBFNzpjqX4oj/RYIQLKXWWfkTKYVLJFZFPCSo00jesw2gieu3Ke/Yy4gwhtNodA
v+s6QNMvffTW/K3XNrWDB0E7/LXbdidzhm+MBu8ov2tuC3tp9liLICiE1jv/2xT4
CNSO6yphmk1/1zEYHS/mN9qJ2csBmte2cdmGyOcuVEHk3pyINNMDOamaURBJGRwF
XB5V7gTKUFU4jCp3chywKrBHJHxGGDUmPBmZtDtfWAOgL32drK7/KUyzZL/WO7Fj
akOI0hRDFOcqTYWL20H7+hAiX3oHMP7eou3L5C7wJ9+JMcACklN/WMjG9a536DFJ
4UgZ6HyKPP+wy837Hbe8b25kNMBwFgiaLR0lcgzxj7NyQWjVCMOEN+M55tRCjvL6
ya6JVZCRbMXfdCy8lVPgtNQ6VlHaj8Wvnn2FLbWWO2n2r3s=
=9zU5
-----END PGP PRIVATE KEY BLOCK-----
"""

    NON_DEFAULT_KEY_ID = "6A93393F50C5E6ACD3D6FB45B936212EDB4E14C0"

    def setUp(self) -> None:
        super().setUp()
        self.gpg_dir = os.path.join(self.test_dir, "gpg")
        os.mkdir(self.gpg_dir, mode=0o700)
        # Ignore errors when deleting GNUPGHOME, because of race conditions
        # (e.g. the gpg-agent socket having been deleted). See
        # https://github.com/jelmer/dulwich/issues/1000
        self.addCleanup(shutil.rmtree, self.gpg_dir, ignore_errors=True)
        self.overrideEnv("GNUPGHOME", self.gpg_dir)

    def import_default_key(self) -> None:
        subprocess.run(
            ["gpg", "--import"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            input=PorcelainGpgTestCase.DEFAULT_KEY,
            text=True,
        )

    def import_non_default_key(self) -> None:
        subprocess.run(
            ["gpg", "--import"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            input=PorcelainGpgTestCase.NON_DEFAULT_KEY,
            text=True,
        )


class ArchiveTests(PorcelainTestCase):
    """Tests for the archive command."""

    def test_simple(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"refs/heads/master"] = c3.id
        out = BytesIO()
        err = BytesIO()
        porcelain.archive(
            self.repo.path, b"refs/heads/master", outstream=out, errstream=err
        )
        self.assertEqual(b"", err.getvalue())
        tf = tarfile.TarFile(fileobj=out)
        self.addCleanup(tf.close)
        self.assertEqual([], tf.getnames())


class UpdateServerInfoTests(PorcelainTestCase):
    def test_simple(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"refs/heads/foo"] = c3.id
        porcelain.update_server_info(self.repo.path)
        self.assertTrue(
            os.path.exists(os.path.join(self.repo.controldir(), "info", "refs"))
        )


class CommitTests(PorcelainTestCase):
    def test_custom_author(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"refs/heads/foo"] = c3.id
        sha = porcelain.commit(
            self.repo.path,
            message=b"Some message",
            author=b"Joe <joe@example.com>",
            committer=b"Bob <bob@example.com>",
        )
        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

    def test_unicode(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"refs/heads/foo"] = c3.id
        sha = porcelain.commit(
            self.repo.path,
            message="Some message",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
        )
        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

    def test_no_verify(self) -> None:
        if os.name != "posix":
            self.skipTest("shell hook tests requires POSIX shell")
        self.assertTrue(os.path.exists("/bin/sh"))

        hooks_dir = os.path.join(self.repo.controldir(), "hooks")
        os.makedirs(hooks_dir, exist_ok=True)
        self.addCleanup(shutil.rmtree, hooks_dir)

        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )

        hook_fail = "#!/bin/sh\nexit 1"

        # hooks are executed in pre-commit, commit-msg order
        # test commit-msg failure first, then pre-commit failure, then
        # no_verify to skip both hooks
        commit_msg = os.path.join(hooks_dir, "commit-msg")
        with open(commit_msg, "w") as f:
            f.write(hook_fail)
        os.chmod(commit_msg, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        with self.assertRaises(CommitError):
            porcelain.commit(
                self.repo.path,
                message="Some message",
                author="Joe <joe@example.com>",
                committer="Bob <bob@example.com>",
            )

        pre_commit = os.path.join(hooks_dir, "pre-commit")
        with open(pre_commit, "w") as f:
            f.write(hook_fail)
        os.chmod(pre_commit, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        with self.assertRaises(CommitError):
            porcelain.commit(
                self.repo.path,
                message="Some message",
                author="Joe <joe@example.com>",
                committer="Bob <bob@example.com>",
            )

        sha = porcelain.commit(
            self.repo.path,
            message="Some message",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
            no_verify=True,
        )
        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

    def test_timezone(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"refs/heads/foo"] = c3.id
        sha = porcelain.commit(
            self.repo.path,
            message="Some message",
            author="Joe <joe@example.com>",
            author_timezone=18000,
            committer="Bob <bob@example.com>",
            commit_timezone=18000,
        )
        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        self.assertEqual(commit._author_timezone, 18000)
        self.assertEqual(commit._commit_timezone, 18000)

        self.overrideEnv("GIT_AUTHOR_DATE", "1995-11-20T19:12:08-0501")
        self.overrideEnv("GIT_COMMITTER_DATE", "1995-11-20T19:12:08-0501")

        sha = porcelain.commit(
            self.repo.path,
            message="Some message",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
        )
        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        self.assertEqual(commit._author_timezone, -18060)
        self.assertEqual(commit._commit_timezone, -18060)

        self.overrideEnv("GIT_AUTHOR_DATE", None)
        self.overrideEnv("GIT_COMMITTER_DATE", None)

        local_timezone = time.localtime().tm_gmtoff

        sha = porcelain.commit(
            self.repo.path,
            message="Some message",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
        )
        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        self.assertEqual(commit._author_timezone, local_timezone)
        self.assertEqual(commit._commit_timezone, local_timezone)


@skipIf(
    platform.python_implementation() == "PyPy" or sys.platform == "win32",
    "gpgme not easily available or supported on Windows and PyPy",
)
class CommitSignTests(PorcelainGpgTestCase):
    def test_default_key(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        self.import_default_key()

        sha = porcelain.commit(
            self.repo.path,
            message="Some message",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
            signoff=True,
        )
        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        # GPG Signatures aren't deterministic, so we can't do a static assertion.
        commit.verify()
        commit.verify(keyids=[PorcelainGpgTestCase.DEFAULT_KEY_ID])

        self.import_non_default_key()
        self.assertRaises(
            gpg.errors.MissingSignatures,
            commit.verify,
            keyids=[PorcelainGpgTestCase.NON_DEFAULT_KEY_ID],
        )

        assert isinstance(commit, Commit)
        commit.committer = b"Alice <alice@example.com>"
        self.assertRaises(
            gpg.errors.BadSignatures,
            commit.verify,
        )

    def test_non_default_key(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        self.import_non_default_key()

        sha = porcelain.commit(
            self.repo.path,
            message="Some message",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
            signoff=PorcelainGpgTestCase.NON_DEFAULT_KEY_ID,
        )
        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        # GPG Signatures aren't deterministic, so we can't do a static assertion.
        commit.verify()

    def test_sign_uses_config_signingkey(self) -> None:
        """Test that sign=True uses user.signingKey from config."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up user.signingKey in config
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        cfg.write_to_path()

        self.import_default_key()

        # Create commit with sign=True (should use signingKey from config)
        sha = porcelain.commit(
            self.repo.path,
            message="Signed with configured key",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
            signoff=True,  # This should read user.signingKey from config
        )

        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        # Verify the commit is signed with the configured key
        commit.verify()
        commit.verify(keyids=[PorcelainGpgTestCase.DEFAULT_KEY_ID])

    def test_commit_gpg_sign_config_enabled(self) -> None:
        """Test that commit.gpgSign=true automatically signs commits."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up user.signingKey and commit.gpgSign in config
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        cfg.set(("commit",), "gpgSign", True)
        cfg.write_to_path()

        self.import_default_key()

        # Create commit without explicit signoff parameter (should auto-sign due to config)
        sha = porcelain.commit(
            self.repo.path,
            message="Auto-signed commit",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
            # No signoff parameter - should use commit.gpgSign config
        )

        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        # Verify the commit is signed due to config
        commit.verify()
        commit.verify(keyids=[PorcelainGpgTestCase.DEFAULT_KEY_ID])

    def test_commit_gpg_sign_config_disabled(self) -> None:
        """Test that commit.gpgSign=false does not sign commits."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up user.signingKey and commit.gpgSign=false in config
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        cfg.set(("commit",), "gpgSign", False)
        cfg.write_to_path()

        self.import_default_key()

        # Create commit without explicit signoff parameter (should not sign)
        sha = porcelain.commit(
            self.repo.path,
            message="Unsigned commit",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
            # No signoff parameter - should use commit.gpgSign=false config
        )

        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        # Verify the commit is not signed
        self.assertIsNone(commit._gpgsig)

    def test_commit_gpg_sign_config_no_signing_key(self) -> None:
        """Test that commit.gpgSign=true works without user.signingKey (uses default)."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up commit.gpgSign but no user.signingKey
        cfg = self.repo.get_config()
        cfg.set(("commit",), "gpgSign", True)
        cfg.write_to_path()

        self.import_default_key()

        # Create commit without explicit signoff parameter (should auto-sign with default key)
        sha = porcelain.commit(
            self.repo.path,
            message="Default signed commit",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
            # No signoff parameter - should use commit.gpgSign config with default key
        )

        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        # Verify the commit is signed with default key
        commit.verify()

    def test_explicit_signoff_overrides_config(self) -> None:
        """Test that explicit signoff parameter overrides commit.gpgSign config."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up commit.gpgSign=false but explicitly pass signoff=True
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        cfg.set(("commit",), "gpgSign", False)
        cfg.write_to_path()

        self.import_default_key()

        # Create commit with explicit signoff=True (should override config)
        sha = porcelain.commit(
            self.repo.path,
            message="Explicitly signed commit",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
            signoff=True,  # This should override commit.gpgSign=false
        )

        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        # Verify the commit is signed despite config=false
        commit.verify()
        commit.verify(keyids=[PorcelainGpgTestCase.DEFAULT_KEY_ID])

    def test_explicit_false_disables_signing(self) -> None:
        """Test that explicit signoff=False disables signing even with config=true."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up commit.gpgSign=true but explicitly pass signoff=False
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        cfg.set(("commit",), "gpgSign", True)
        cfg.write_to_path()

        self.import_default_key()

        # Create commit with explicit signoff=False (should disable signing)
        sha = porcelain.commit(
            self.repo.path,
            message="Explicitly unsigned commit",
            author="Joe <joe@example.com>",
            committer="Bob <bob@example.com>",
            signoff=False,  # This should override commit.gpgSign=true
        )

        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)

        commit = self.repo.get_object(sha)
        assert isinstance(commit, Commit)
        # Verify the commit is NOT signed despite config=true
        self.assertIsNone(commit._gpgsig)


class TimezoneTests(PorcelainTestCase):
    def put_envs(self, value) -> None:
        self.overrideEnv("GIT_AUTHOR_DATE", value)
        self.overrideEnv("GIT_COMMITTER_DATE", value)

    def fallback(self, value) -> None:
        self.put_envs(value)
        self.assertRaises(porcelain.TimezoneFormatError, porcelain.get_user_timezones)

    def test_internal_format(self) -> None:
        self.put_envs("0 +0500")
        self.assertTupleEqual((18000, 18000), porcelain.get_user_timezones())

    def test_rfc_2822(self) -> None:
        self.put_envs("Mon, 20 Nov 1995 19:12:08 -0500")
        self.assertTupleEqual((-18000, -18000), porcelain.get_user_timezones())

        self.put_envs("Mon, 20 Nov 1995 19:12:08")
        self.assertTupleEqual((0, 0), porcelain.get_user_timezones())

    def test_iso8601(self) -> None:
        self.put_envs("1995-11-20T19:12:08-0501")
        self.assertTupleEqual((-18060, -18060), porcelain.get_user_timezones())

        self.put_envs("1995-11-20T19:12:08+0501")
        self.assertTupleEqual((18060, 18060), porcelain.get_user_timezones())

        self.put_envs("1995-11-20T19:12:08-05:01")
        self.assertTupleEqual((-18060, -18060), porcelain.get_user_timezones())

        self.put_envs("1995-11-20 19:12:08-05")
        self.assertTupleEqual((-18000, -18000), porcelain.get_user_timezones())

        # https://github.com/git/git/blob/96b2d4fa927c5055adc5b1d08f10a5d7352e2989/t/t6300-for-each-ref.sh#L128
        self.put_envs("2006-07-03 17:18:44 +0200")
        self.assertTupleEqual((7200, 7200), porcelain.get_user_timezones())

    def test_missing_or_malformed(self) -> None:
        # TODO: add more here
        self.fallback("0 + 0500")
        self.fallback("a +0500")

        self.fallback("1995-11-20T19:12:08")
        self.fallback("1995-11-20T19:12:08-05:")

        self.fallback("1995.11.20")
        self.fallback("11/20/1995")
        self.fallback("20.11.1995")

    def test_different_envs(self) -> None:
        self.overrideEnv("GIT_AUTHOR_DATE", "0 +0500")
        self.overrideEnv("GIT_COMMITTER_DATE", "0 +0501")
        self.assertTupleEqual((18000, 18060), porcelain.get_user_timezones())

    def test_no_envs(self) -> None:
        local_timezone = time.localtime().tm_gmtoff

        self.put_envs("0 +0500")
        self.assertTupleEqual((18000, 18000), porcelain.get_user_timezones())

        self.overrideEnv("GIT_COMMITTER_DATE", None)
        self.assertTupleEqual((18000, local_timezone), porcelain.get_user_timezones())

        self.put_envs("0 +0500")
        self.overrideEnv("GIT_AUTHOR_DATE", None)
        self.assertTupleEqual((local_timezone, 18000), porcelain.get_user_timezones())

        self.put_envs("0 +0500")
        self.overrideEnv("GIT_AUTHOR_DATE", None)
        self.overrideEnv("GIT_COMMITTER_DATE", None)
        self.assertTupleEqual(
            (local_timezone, local_timezone), porcelain.get_user_timezones()
        )


class CleanTests(PorcelainTestCase):
    def put_files(self, tracked, ignored, untracked, empty_dirs) -> None:
        """Put the described files in the wd."""
        all_files = tracked | ignored | untracked
        for file_path in all_files:
            abs_path = os.path.join(self.repo.path, file_path)
            # File may need to be written in a dir that doesn't exist yet, so
            # create the parent dir(s) as necessary
            parent_dir = os.path.dirname(abs_path)
            try:
                os.makedirs(parent_dir)
            except FileExistsError:
                pass
            with open(abs_path, "w") as f:
                f.write("")

        with open(os.path.join(self.repo.path, ".gitignore"), "w") as f:
            f.writelines(ignored)

        for dir_path in empty_dirs:
            os.mkdir(os.path.join(self.repo.path, "empty_dir"))

        files_to_add = [os.path.join(self.repo.path, t) for t in tracked]
        porcelain.add(repo=self.repo.path, paths=files_to_add)
        porcelain.commit(repo=self.repo.path, message="init commit")

    def assert_wd(self, expected_paths) -> None:
        """Assert paths of files and dirs in wd are same as expected_paths."""
        control_dir_rel = os.path.relpath(self.repo._controldir, self.repo.path)

        # normalize paths to simplify comparison across platforms
        found_paths = {
            os.path.normpath(p)
            for p in flat_walk_dir(self.repo.path)
            if not p.split(os.sep)[0] == control_dir_rel
        }
        norm_expected_paths = {os.path.normpath(p) for p in expected_paths}
        self.assertEqual(found_paths, norm_expected_paths)

    def test_from_root(self) -> None:
        self.put_files(
            tracked={"tracked_file", "tracked_dir/tracked_file", ".gitignore"},
            ignored={"ignored_file"},
            untracked={
                "untracked_file",
                "tracked_dir/untracked_dir/untracked_file",
                "untracked_dir/untracked_dir/untracked_file",
            },
            empty_dirs={"empty_dir"},
        )

        porcelain.clean(repo=self.repo.path, target_dir=self.repo.path)

        self.assert_wd(
            {
                "tracked_file",
                "tracked_dir/tracked_file",
                ".gitignore",
                "ignored_file",
                "tracked_dir",
            }
        )

    def test_from_subdir(self) -> None:
        self.put_files(
            tracked={"tracked_file", "tracked_dir/tracked_file", ".gitignore"},
            ignored={"ignored_file"},
            untracked={
                "untracked_file",
                "tracked_dir/untracked_dir/untracked_file",
                "untracked_dir/untracked_dir/untracked_file",
            },
            empty_dirs={"empty_dir"},
        )

        porcelain.clean(
            repo=self.repo,
            target_dir=os.path.join(self.repo.path, "untracked_dir"),
        )

        self.assert_wd(
            {
                "tracked_file",
                "tracked_dir/tracked_file",
                ".gitignore",
                "ignored_file",
                "untracked_file",
                "tracked_dir/untracked_dir/untracked_file",
                "empty_dir",
                "untracked_dir",
                "tracked_dir",
                "tracked_dir/untracked_dir",
            }
        )


class CloneTests(PorcelainTestCase):
    def test_simple_local(self) -> None:
        f1_1 = make_object(Blob, data=b"f1")
        commit_spec = [[1], [2, 1], [3, 1, 2]]
        trees = {
            1: [(b"f1", f1_1), (b"f2", f1_1)],
            2: [(b"f1", f1_1), (b"f2", f1_1)],
            3: [(b"f1", f1_1), (b"f2", f1_1)],
        }

        c1, c2, c3 = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c3.id
        self.repo.refs[b"refs/tags/foo"] = c3.id
        target_path = tempfile.mkdtemp()
        errstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        r = porcelain.clone(
            self.repo.path, target_path, checkout=False, errstream=errstream
        )
        self.addCleanup(r.close)
        self.assertEqual(r.path, target_path)
        target_repo = Repo(target_path)
        self.assertEqual(0, len(target_repo.open_index()))
        self.assertEqual(c3.id, target_repo.refs[b"refs/tags/foo"])
        self.assertNotIn(b"f1", os.listdir(target_path))
        self.assertNotIn(b"f2", os.listdir(target_path))
        c = r.get_config()
        encoded_path = self.repo.path
        if not isinstance(encoded_path, bytes):
            encoded_path_bytes = encoded_path.encode("utf-8")
        else:
            encoded_path_bytes = encoded_path
        self.assertEqual(encoded_path_bytes, c.get((b"remote", b"origin"), b"url"))
        self.assertEqual(
            b"+refs/heads/*:refs/remotes/origin/*",
            c.get((b"remote", b"origin"), b"fetch"),
        )

    def test_simple_local_with_checkout(self) -> None:
        f1_1 = make_object(Blob, data=b"f1")
        commit_spec = [[1], [2, 1], [3, 1, 2]]
        trees = {
            1: [(b"f1", f1_1), (b"f2", f1_1)],
            2: [(b"f1", f1_1), (b"f2", f1_1)],
            3: [(b"f1", f1_1), (b"f2", f1_1)],
        }

        c1, c2, c3 = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c3.id
        target_path = tempfile.mkdtemp()
        errstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        with porcelain.clone(
            self.repo.path, target_path, checkout=True, errstream=errstream
        ) as r:
            self.assertEqual(r.path, target_path)
        with Repo(target_path) as r:
            self.assertEqual(r.head(), c3.id)
        self.assertIn("f1", os.listdir(target_path))
        self.assertIn("f2", os.listdir(target_path))

    def test_bare_local_with_checkout(self) -> None:
        f1_1 = make_object(Blob, data=b"f1")
        commit_spec = [[1], [2, 1], [3, 1, 2]]
        trees = {
            1: [(b"f1", f1_1), (b"f2", f1_1)],
            2: [(b"f1", f1_1), (b"f2", f1_1)],
            3: [(b"f1", f1_1), (b"f2", f1_1)],
        }

        c1, c2, c3 = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c3.id
        target_path = tempfile.mkdtemp()
        errstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        with porcelain.clone(
            self.repo.path, target_path, bare=True, errstream=errstream
        ) as r:
            self.assertEqual(r.path, target_path)
        with Repo(target_path) as r:
            r.head()
            self.assertRaises(NoIndexPresent, r.open_index)
        self.assertNotIn(b"f1", os.listdir(target_path))
        self.assertNotIn(b"f2", os.listdir(target_path))

    def test_no_checkout_with_bare(self) -> None:
        f1_1 = make_object(Blob, data=b"f1")
        commit_spec = [[1]]
        trees = {1: [(b"f1", f1_1), (b"f2", f1_1)]}

        (c1,) = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c1.id
        self.repo.refs[b"HEAD"] = c1.id
        target_path = tempfile.mkdtemp()
        errstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        self.assertRaises(
            porcelain.Error,
            porcelain.clone,
            self.repo.path,
            target_path,
            checkout=True,
            bare=True,
            errstream=errstream,
        )

    def test_no_head_no_checkout(self) -> None:
        f1_1 = make_object(Blob, data=b"f1")
        commit_spec = [[1]]
        trees = {1: [(b"f1", f1_1), (b"f2", f1_1)]}

        (c1,) = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c1.id
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        errstream = BytesIO()
        r = porcelain.clone(
            self.repo.path, target_path, checkout=True, errstream=errstream
        )
        r.close()

    def test_no_head_no_checkout_outstream_errstream_autofallback(self) -> None:
        f1_1 = make_object(Blob, data=b"f1")
        commit_spec = [[1]]
        trees = {1: [(b"f1", f1_1), (b"f2", f1_1)]}

        (c1,) = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c1.id
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        errstream = porcelain.NoneStream()
        r = porcelain.clone(
            self.repo.path, target_path, checkout=True, errstream=errstream
        )
        r.close()

    def test_source_broken(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            target_path = os.path.join(parent, "target")
            self.assertRaises(
                Exception, porcelain.clone, "/nonexistent/repo", target_path
            )
            self.assertFalse(os.path.exists(target_path))

    def test_fetch_symref(self) -> None:
        f1_1 = make_object(Blob, data=b"f1")
        trees = {1: [(b"f1", f1_1), (b"f2", f1_1)]}
        [c1] = build_commit_graph(self.repo.object_store, [[1]], trees)
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/else")
        self.repo.refs[b"refs/heads/else"] = c1.id
        target_path = tempfile.mkdtemp()
        errstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        r = porcelain.clone(
            self.repo.path, target_path, checkout=False, errstream=errstream
        )
        self.addCleanup(r.close)
        self.assertEqual(r.path, target_path)
        target_repo = Repo(target_path)
        self.assertEqual(0, len(target_repo.open_index()))
        self.assertEqual(c1.id, target_repo.refs[b"refs/heads/else"])
        self.assertEqual(c1.id, target_repo.refs[b"HEAD"])
        self.assertEqual(
            {
                b"HEAD": b"refs/heads/else",
                b"refs/remotes/origin/HEAD": b"refs/remotes/origin/else",
            },
            target_repo.refs.get_symrefs(),
        )

    def test_detached_head(self) -> None:
        f1_1 = make_object(Blob, data=b"f1")
        commit_spec = [[1], [2, 1], [3, 1, 2]]
        trees = {
            1: [(b"f1", f1_1), (b"f2", f1_1)],
            2: [(b"f1", f1_1), (b"f2", f1_1)],
            3: [(b"f1", f1_1), (b"f2", f1_1)],
        }

        c1, c2, c3 = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c2.id
        self.repo.refs.remove_if_equals(b"HEAD", None)
        self.repo.refs[b"HEAD"] = c3.id
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        errstream = porcelain.NoneStream()
        with porcelain.clone(
            self.repo.path, target_path, checkout=True, errstream=errstream
        ) as r:
            self.assertEqual(c3.id, r.refs[b"HEAD"])

    def test_clone_pathlib(self) -> None:
        from pathlib import Path

        f1_1 = make_object(Blob, data=b"f1")
        commit_spec = [[1]]
        trees = {1: [(b"f1", f1_1)]}

        c1 = build_commit_graph(self.repo.object_store, commit_spec, trees)[0]
        self.repo.refs[b"refs/heads/master"] = c1.id

        target_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_dir)
        target_path = Path(target_dir) / "clone_repo"

        errstream = BytesIO()
        r = porcelain.clone(
            self.repo.path, target_path, checkout=False, errstream=errstream
        )
        self.addCleanup(r.close)
        self.assertEqual(r.path, str(target_path))
        self.assertTrue(os.path.exists(str(target_path)))

    def test_clone_with_recurse_submodules(self) -> None:
        # Create a submodule repository
        sub_repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, sub_repo_path)
        sub_repo = Repo.init(sub_repo_path)
        self.addCleanup(sub_repo.close)

        # Add a file to the submodule repo
        sub_file = os.path.join(sub_repo_path, "subfile.txt")
        with open(sub_file, "w") as f:
            f.write("submodule content")

        porcelain.add(sub_repo, paths=[sub_file])
        sub_commit = porcelain.commit(
            sub_repo,
            message=b"Initial submodule commit",
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
        )

        # Create main repository with submodule
        main_file = os.path.join(self.repo.path, "main.txt")
        with open(main_file, "w") as f:
            f.write("main content")

        porcelain.add(self.repo, paths=[main_file])
        porcelain.submodule_add(self.repo, sub_repo_path, "sub")

        # Manually add the submodule to the index since submodule_add doesn't do it
        # when the repository is local (to maintain backward compatibility)
        from dulwich.index import IndexEntry
        from dulwich.objects import S_IFGITLINK

        index = self.repo.open_index()
        index[b"sub"] = IndexEntry(
            ctime=0,
            mtime=0,
            dev=0,
            ino=0,
            mode=S_IFGITLINK,
            uid=0,
            gid=0,
            size=0,
            sha=sub_commit,
            flags=0,
        )
        index.write()

        porcelain.add(self.repo, paths=[".gitmodules"])
        porcelain.commit(
            self.repo,
            message=b"Add submodule",
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
        )

        # Clone with recurse_submodules
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)

        cloned = porcelain.clone(
            self.repo.path,
            target_path,
            recurse_submodules=True,
        )
        self.addCleanup(cloned.close)

        # Check main file exists
        cloned_main = os.path.join(target_path, "main.txt")
        self.assertTrue(os.path.exists(cloned_main))
        with open(cloned_main) as f:
            self.assertEqual(f.read(), "main content")

        # Check submodule file exists
        cloned_sub_file = os.path.join(target_path, "sub", "subfile.txt")
        self.assertTrue(os.path.exists(cloned_sub_file))
        with open(cloned_sub_file) as f:
            self.assertEqual(f.read(), "submodule content")


class InitTests(TestCase):
    def test_non_bare(self) -> None:
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        porcelain.init(repo_dir)

    def test_bare(self) -> None:
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        porcelain.init(repo_dir, bare=True)

    def test_init_pathlib(self) -> None:
        from pathlib import Path

        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        repo_path = Path(repo_dir)

        # Test non-bare repo with pathlib
        repo = porcelain.init(repo_path)
        self.assertTrue(os.path.exists(os.path.join(repo_dir, ".git")))
        repo.close()

    def test_init_bare_pathlib(self) -> None:
        from pathlib import Path

        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        repo_path = Path(repo_dir)

        # Test bare repo with pathlib
        repo = porcelain.init(repo_path, bare=True)
        self.assertTrue(os.path.exists(os.path.join(repo_dir, "refs")))
        repo.close()


class AddTests(PorcelainTestCase):
    def test_add_default_paths(self) -> None:
        # create a file for initial commit
        fullpath = os.path.join(self.repo.path, "blah")
        with open(fullpath, "w") as f:
            f.write("\n")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo.path,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Add a second test file and a file in a directory
        with open(os.path.join(self.repo.path, "foo"), "w") as f:
            f.write("\n")
        os.mkdir(os.path.join(self.repo.path, "adir"))
        with open(os.path.join(self.repo.path, "adir", "afile"), "w") as f:
            f.write("\n")
        cwd = os.getcwd()
        self.addCleanup(os.chdir, cwd)
        os.chdir(self.repo.path)
        self.assertEqual({"foo", "blah", "adir", ".git"}, set(os.listdir(".")))
        added, ignored = porcelain.add(self.repo.path)
        # Normalize paths to use forward slashes for comparison
        added_normalized = [path.replace(os.sep, "/") for path in added]
        self.assertEqual(
            (added_normalized, ignored),
            (["foo", "adir/afile"], set()),
        )

        # Check that foo was added and nothing in .git was modified
        index = self.repo.open_index()
        self.assertEqual(sorted(index), [b"adir/afile", b"blah", b"foo"])

    def test_add_default_paths_subdir(self) -> None:
        os.mkdir(os.path.join(self.repo.path, "foo"))
        with open(os.path.join(self.repo.path, "blah"), "w") as f:
            f.write("\n")
        with open(os.path.join(self.repo.path, "foo", "blie"), "w") as f:
            f.write("\n")

        cwd = os.getcwd()
        self.addCleanup(os.chdir, cwd)
        os.chdir(os.path.join(self.repo.path, "foo"))
        porcelain.add(repo=self.repo.path)
        porcelain.commit(
            repo=self.repo.path,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        index = self.repo.open_index()
        # After fix: add() with no paths should behave like git add -A (add everything)
        self.assertEqual(sorted(index), [b"blah", b"foo/blie"])

    def test_add_file(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        self.assertIn(b"foo", self.repo.open_index())

    def test_add_ignored(self) -> None:
        with open(os.path.join(self.repo.path, ".gitignore"), "w") as f:
            f.write("foo\nsubdir/")
        with open(os.path.join(self.repo.path, "foo"), "w") as f:
            f.write("BAR")
        with open(os.path.join(self.repo.path, "bar"), "w") as f:
            f.write("BAR")
        os.mkdir(os.path.join(self.repo.path, "subdir"))
        with open(os.path.join(self.repo.path, "subdir", "baz"), "w") as f:
            f.write("BAZ")
        (added, ignored) = porcelain.add(
            self.repo.path,
            paths=[
                os.path.join(self.repo.path, "foo"),
                os.path.join(self.repo.path, "bar"),
                os.path.join(self.repo.path, "subdir"),
            ],
        )
        self.assertIn(b"bar", self.repo.open_index())
        self.assertEqual({"bar"}, set(added))
        self.assertEqual({"foo", "subdir/"}, ignored)

    def test_add_from_ignored_directory(self) -> None:
        # Test for issue #550 - adding files when cwd is in ignored directory
        # Create .gitignore that ignores build/
        with open(os.path.join(self.repo.path, ".gitignore"), "w") as f:
            f.write("build/\n")

        # Create ignored directory
        build_dir = os.path.join(self.repo.path, "build")
        os.mkdir(build_dir)

        # Create a file in the repo (not in ignored directory)
        src_file = os.path.join(self.repo.path, "source.py")
        with open(src_file, "w") as f:
            f.write("print('hello')\n")

        # Save current directory and change to ignored directory
        original_cwd = os.getcwd()
        try:
            os.chdir(build_dir)
            # Add file using absolute path from within ignored directory
            (added, ignored) = porcelain.add(self.repo.path, paths=[src_file])
            self.assertIn(b"source.py", self.repo.open_index())
            self.assertEqual({"source.py"}, set(added))
        finally:
            os.chdir(original_cwd)

    def test_add_file_absolute_path(self) -> None:
        # Absolute paths are (not yet) supported
        with open(os.path.join(self.repo.path, "foo"), "w") as f:
            f.write("BAR")
        porcelain.add(self.repo, paths=[os.path.join(self.repo.path, "foo")])
        self.assertIn(b"foo", self.repo.open_index())

    def test_add_not_in_repo(self) -> None:
        with open(os.path.join(self.test_dir, "foo"), "w") as f:
            f.write("BAR")
        self.assertRaises(
            ValueError,
            porcelain.add,
            self.repo,
            paths=[os.path.join(self.test_dir, "foo")],
        )
        self.assertRaises(
            (ValueError, FileNotFoundError),
            porcelain.add,
            self.repo,
            paths=["../foo"],
        )
        self.assertEqual([], list(self.repo.open_index()))

    def test_add_file_clrf_conversion(self) -> None:
        from dulwich.index import IndexEntry

        # Set the right configuration to the repo
        c = self.repo.get_config()
        c.set("core", "autocrlf", "input")
        c.write_to_path()

        # Add a file with CRLF line-ending
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "wb") as f:
            f.write(b"line1\r\nline2")
        porcelain.add(self.repo.path, paths=[fullpath])

        # The line-endings should have been converted to LF
        index = self.repo.open_index()
        self.assertIn(b"foo", index)

        entry = index[b"foo"]
        assert isinstance(entry, IndexEntry)
        blob = self.repo[entry.sha]
        self.assertEqual(blob.data, b"line1\nline2")

    def test_add_symlink_outside_repo(self) -> None:
        """Test adding a symlink that points outside the repository."""
        # Create a symlink pointing outside the repository
        symlink_path = os.path.join(self.repo.path, "symlink_to_nowhere")
        os.symlink("/nonexistent/path", symlink_path)

        # Adding the symlink should succeed (matching Git's behavior)
        added, ignored = porcelain.add(self.repo.path, paths=[symlink_path])

        # Should successfully add the symlink
        self.assertIn("symlink_to_nowhere", added)
        self.assertEqual(len(ignored), 0)

        # Verify symlink is actually staged
        index = self.repo.open_index()
        self.assertIn(b"symlink_to_nowhere", index)

    def test_add_symlink_to_file_inside_repo(self) -> None:
        """Test adding a symlink that points to a file inside the repository."""
        # Create a regular file
        target_file = os.path.join(self.repo.path, "target.txt")
        with open(target_file, "w") as f:
            f.write("target content")

        # Create a symlink to the file
        symlink_path = os.path.join(self.repo.path, "link_to_target")
        os.symlink("target.txt", symlink_path)

        # Add both the target and the symlink
        added, ignored = porcelain.add(
            self.repo.path, paths=[target_file, symlink_path]
        )

        # Both should be added successfully
        self.assertIn("target.txt", added)
        self.assertIn("link_to_target", added)
        self.assertEqual(len(ignored), 0)

        # Verify both are in the index
        index = self.repo.open_index()
        self.assertIn(b"target.txt", index)
        self.assertIn(b"link_to_target", index)

    def test_add_symlink_to_directory_inside_repo(self) -> None:
        """Test adding a symlink that points to a directory inside the repository."""
        # Create a directory with some files
        target_dir = os.path.join(self.repo.path, "target_dir")
        os.mkdir(target_dir)
        with open(os.path.join(target_dir, "file1.txt"), "w") as f:
            f.write("content1")
        with open(os.path.join(target_dir, "file2.txt"), "w") as f:
            f.write("content2")

        # Create a symlink to the directory
        symlink_path = os.path.join(self.repo.path, "link_to_dir")
        os.symlink("target_dir", symlink_path)

        # Add the symlink
        added, ignored = porcelain.add(self.repo.path, paths=[symlink_path])

        # When adding a symlink to a directory, it follows the symlink and adds contents
        self.assertEqual(len(added), 2)
        self.assertIn("link_to_dir/file1.txt", added)
        self.assertIn("link_to_dir/file2.txt", added)
        self.assertEqual(len(ignored), 0)

        # Verify files are added through the symlink path
        index = self.repo.open_index()
        self.assertIn(b"link_to_dir/file1.txt", index)
        self.assertIn(b"link_to_dir/file2.txt", index)
        # The original target directory files are not added
        self.assertNotIn(b"target_dir/file1.txt", index)
        self.assertNotIn(b"target_dir/file2.txt", index)

    def test_add_symlink_chain(self) -> None:
        """Test adding a chain of symlinks (symlink to symlink)."""
        # Create a regular file
        target_file = os.path.join(self.repo.path, "original.txt")
        with open(target_file, "w") as f:
            f.write("original content")

        # Create first symlink
        first_link = os.path.join(self.repo.path, "link1")
        os.symlink("original.txt", first_link)

        # Create second symlink pointing to first
        second_link = os.path.join(self.repo.path, "link2")
        os.symlink("link1", second_link)

        # Add all files
        added, ignored = porcelain.add(
            self.repo.path, paths=[target_file, first_link, second_link]
        )

        # All should be added
        self.assertEqual(len(added), 3)
        self.assertIn("original.txt", added)
        self.assertIn("link1", added)
        self.assertIn("link2", added)

        # Verify all are in the index
        index = self.repo.open_index()
        self.assertIn(b"original.txt", index)
        self.assertIn(b"link1", index)
        self.assertIn(b"link2", index)

    def test_add_broken_symlink(self) -> None:
        """Test adding a broken symlink (points to non-existent target)."""
        # Create a symlink to a non-existent file
        broken_link = os.path.join(self.repo.path, "broken_link")
        os.symlink("does_not_exist.txt", broken_link)

        # Add the broken symlink
        added, ignored = porcelain.add(self.repo.path, paths=[broken_link])

        # Should be added successfully (Git tracks the symlink, not its target)
        self.assertIn("broken_link", added)
        self.assertEqual(len(ignored), 0)

        # Verify it's in the index
        index = self.repo.open_index()
        self.assertIn(b"broken_link", index)

    def test_add_symlink_relative_outside_repo(self) -> None:
        """Test adding a symlink that uses '..' to point outside the repository."""
        # Create a file outside the repo
        outside_file = os.path.join(self.test_dir, "outside.txt")
        with open(outside_file, "w") as f:
            f.write("outside content")

        # Create a symlink using relative path to go outside
        symlink_path = os.path.join(self.repo.path, "link_outside")
        os.symlink("../outside.txt", symlink_path)

        # Add the symlink
        added, ignored = porcelain.add(self.repo.path, paths=[symlink_path])

        # Should be added successfully
        self.assertIn("link_outside", added)
        self.assertEqual(len(ignored), 0)

        # Verify it's in the index
        index = self.repo.open_index()
        self.assertIn(b"link_outside", index)

    def test_add_symlink_absolute_to_system(self) -> None:
        """Test adding a symlink with absolute path to system directory."""
        # Create a symlink to a system directory
        symlink_path = os.path.join(self.repo.path, "link_to_tmp")
        if os.name == "nt":
            # On Windows, use a system directory like TEMP
            symlink_target = os.environ["TEMP"]
        else:
            # On Unix-like systems, use /tmp
            symlink_target = "/tmp"
        os.symlink(symlink_target, symlink_path)

        # Adding a symlink to a directory outside the repo should raise ValueError
        with self.assertRaises(ValueError) as cm:
            porcelain.add(self.repo.path, paths=[symlink_path])

        # Check that the error indicates the path is outside the repository
        self.assertIn("is not in the subpath of", str(cm.exception))

    def test_add_file_through_symlink(self) -> None:
        """Test adding a file through a symlinked directory."""
        # Create a directory with a file
        real_dir = os.path.join(self.repo.path, "real_dir")
        os.mkdir(real_dir)
        real_file = os.path.join(real_dir, "file.txt")
        with open(real_file, "w") as f:
            f.write("content")

        # Create a symlink to the directory
        link_dir = os.path.join(self.repo.path, "link_dir")
        os.symlink("real_dir", link_dir)

        # Try to add the file through the symlink path
        symlink_file_path = os.path.join(link_dir, "file.txt")

        # This should add the real file, not create a new entry
        added, ignored = porcelain.add(self.repo.path, paths=[symlink_file_path])

        # The real file should be added
        self.assertIn("real_dir/file.txt", added)
        self.assertEqual(len(added), 1)

        # Verify correct path in index
        index = self.repo.open_index()
        self.assertIn(b"real_dir/file.txt", index)
        # Should not create a separate entry for the symlink path
        self.assertNotIn(b"link_dir/file.txt", index)

    def test_add_repo_path(self) -> None:
        """Test adding the repository path itself should add all untracked files."""
        # Create some untracked files
        with open(os.path.join(self.repo.path, "file1.txt"), "w") as f:
            f.write("content1")
        with open(os.path.join(self.repo.path, "file2.txt"), "w") as f:
            f.write("content2")

        # Add the repository path itself
        added, ignored = porcelain.add(self.repo.path, paths=[self.repo.path])

        # Should add all untracked files, not stage './'
        self.assertIn("file1.txt", added)
        self.assertIn("file2.txt", added)
        self.assertNotIn("./", added)

        # Verify files are actually staged
        index = self.repo.open_index()
        self.assertIn(b"file1.txt", index)
        self.assertIn(b"file2.txt", index)

    def test_add_directory_contents(self) -> None:
        """Test adding a directory adds all files within it."""
        # Create a subdirectory with multiple files
        subdir = os.path.join(self.repo.path, "subdir")
        os.mkdir(subdir)
        with open(os.path.join(subdir, "file1.txt"), "w") as f:
            f.write("content1")
        with open(os.path.join(subdir, "file2.txt"), "w") as f:
            f.write("content2")
        with open(os.path.join(subdir, "file3.txt"), "w") as f:
            f.write("content3")

        # Add the directory
        added, ignored = porcelain.add(self.repo.path, paths=["subdir"])

        # Should add all files in the directory
        self.assertEqual(len(added), 3)
        # Normalize paths to use forward slashes for comparison
        added_normalized = [path.replace(os.sep, "/") for path in added]
        self.assertIn("subdir/file1.txt", added_normalized)
        self.assertIn("subdir/file2.txt", added_normalized)
        self.assertIn("subdir/file3.txt", added_normalized)

        # Verify files are actually staged
        index = self.repo.open_index()
        self.assertIn(b"subdir/file1.txt", index)
        self.assertIn(b"subdir/file2.txt", index)
        self.assertIn(b"subdir/file3.txt", index)

    def test_add_nested_directories(self) -> None:
        """Test adding a directory with nested subdirectories."""
        # Create nested directory structure
        dir1 = os.path.join(self.repo.path, "dir1")
        dir2 = os.path.join(dir1, "dir2")
        dir3 = os.path.join(dir2, "dir3")
        os.makedirs(dir3)

        # Add files at each level
        with open(os.path.join(dir1, "file1.txt"), "w") as f:
            f.write("level1")
        with open(os.path.join(dir2, "file2.txt"), "w") as f:
            f.write("level2")
        with open(os.path.join(dir3, "file3.txt"), "w") as f:
            f.write("level3")

        # Add the top-level directory
        added, ignored = porcelain.add(self.repo.path, paths=["dir1"])

        # Should add all files recursively
        self.assertEqual(len(added), 3)
        # Normalize paths to use forward slashes for comparison
        added_normalized = [path.replace(os.sep, "/") for path in added]
        self.assertIn("dir1/file1.txt", added_normalized)
        self.assertIn("dir1/dir2/file2.txt", added_normalized)
        self.assertIn("dir1/dir2/dir3/file3.txt", added_normalized)

        # Verify files are actually staged
        index = self.repo.open_index()
        self.assertIn(b"dir1/file1.txt", index)
        self.assertIn(b"dir1/dir2/file2.txt", index)
        self.assertIn(b"dir1/dir2/dir3/file3.txt", index)

    def test_add_directory_with_tracked_files(self) -> None:
        """Test adding a directory with some files already tracked."""
        # Create a subdirectory with files
        subdir = os.path.join(self.repo.path, "mixed")
        os.mkdir(subdir)

        # Create and commit one file
        tracked_file = os.path.join(subdir, "tracked.txt")
        with open(tracked_file, "w") as f:
            f.write("already tracked")
        porcelain.add(self.repo.path, paths=[tracked_file])
        porcelain.commit(
            repo=self.repo.path,
            message=b"Add tracked file",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Add more untracked files
        with open(os.path.join(subdir, "untracked1.txt"), "w") as f:
            f.write("new file 1")
        with open(os.path.join(subdir, "untracked2.txt"), "w") as f:
            f.write("new file 2")

        # Add the directory
        added, ignored = porcelain.add(self.repo.path, paths=["mixed"])

        # Should only add the untracked files
        self.assertEqual(len(added), 2)
        # Normalize paths to use forward slashes for comparison
        added_normalized = [path.replace(os.sep, "/") for path in added]
        self.assertIn("mixed/untracked1.txt", added_normalized)
        self.assertIn("mixed/untracked2.txt", added_normalized)
        self.assertNotIn("mixed/tracked.txt", added)

        # Verify the index contains all files
        index = self.repo.open_index()
        self.assertIn(b"mixed/tracked.txt", index)
        self.assertIn(b"mixed/untracked1.txt", index)
        self.assertIn(b"mixed/untracked2.txt", index)

    def test_add_directory_with_gitignore(self) -> None:
        """Test adding a directory respects .gitignore patterns."""
        # Create .gitignore
        with open(os.path.join(self.repo.path, ".gitignore"), "w") as f:
            f.write("*.log\n*.tmp\nbuild/\n")

        # Create directory with mixed files
        testdir = os.path.join(self.repo.path, "testdir")
        os.mkdir(testdir)

        # Create various files
        with open(os.path.join(testdir, "important.txt"), "w") as f:
            f.write("keep this")
        with open(os.path.join(testdir, "debug.log"), "w") as f:
            f.write("ignore this")
        with open(os.path.join(testdir, "temp.tmp"), "w") as f:
            f.write("ignore this too")
        with open(os.path.join(testdir, "readme.md"), "w") as f:
            f.write("keep this too")

        # Create a build directory that should be ignored
        builddir = os.path.join(testdir, "build")
        os.mkdir(builddir)
        with open(os.path.join(builddir, "output.txt"), "w") as f:
            f.write("ignore entire directory")

        # Add the directory
        added, ignored = porcelain.add(self.repo.path, paths=["testdir"])

        # Should only add non-ignored files
        # Normalize paths to use forward slashes for comparison
        added_normalized = {path.replace(os.sep, "/") for path in added}
        self.assertEqual(
            added_normalized, {"testdir/important.txt", "testdir/readme.md"}
        )

        # Check ignored files
        # Normalize paths to use forward slashes for comparison
        ignored_normalized = {path.replace(os.sep, "/") for path in ignored}
        self.assertIn("testdir/debug.log", ignored_normalized)
        self.assertIn("testdir/temp.tmp", ignored_normalized)
        self.assertIn("testdir/build/", ignored_normalized)

    def test_add_multiple_directories(self) -> None:
        """Test adding multiple directories in one call."""
        # Create multiple directories
        for dirname in ["dir1", "dir2", "dir3"]:
            dirpath = os.path.join(self.repo.path, dirname)
            os.mkdir(dirpath)
            # Add files to each directory
            for i in range(2):
                with open(os.path.join(dirpath, f"file{i}.txt"), "w") as f:
                    f.write(f"content {dirname} {i}")

        # Add all directories at once
        added, ignored = porcelain.add(self.repo.path, paths=["dir1", "dir2", "dir3"])

        # Should add all files from all directories
        self.assertEqual(len(added), 6)
        # Normalize paths to use forward slashes for comparison
        added_normalized = [path.replace(os.sep, "/") for path in added]
        for dirname in ["dir1", "dir2", "dir3"]:
            for i in range(2):
                self.assertIn(f"{dirname}/file{i}.txt", added_normalized)

        # Verify all files are staged
        index = self.repo.open_index()
        self.assertEqual(len(index), 6)

    def test_add_default_paths_includes_modified_files(self) -> None:
        """Test that add() with no paths includes both untracked and modified files."""
        # Create and commit initial file
        initial_file = os.path.join(self.repo.path, "existing.txt")
        with open(initial_file, "w") as f:
            f.write("initial content\n")
        porcelain.add(repo=self.repo.path, paths=[initial_file])
        porcelain.commit(
            repo=self.repo.path,
            message=b"initial commit",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Modify the existing file (this creates an unstaged change)
        with open(initial_file, "w") as f:
            f.write("modified content\n")

        # Create a new untracked file
        new_file = os.path.join(self.repo.path, "new.txt")
        with open(new_file, "w") as f:
            f.write("new file content\n")

        # Call add() with no paths - should stage both modified and untracked files
        added_files, ignored_files = porcelain.add(repo=self.repo.path)

        # Verify both files were added
        self.assertIn("existing.txt", added_files)
        self.assertIn("new.txt", added_files)
        self.assertEqual(len(ignored_files), 0)

        # Verify both files are now staged
        index = self.repo.open_index()
        self.assertIn(b"existing.txt", index)
        self.assertIn(b"new.txt", index)


class RemoveTests(PorcelainTestCase):
    def test_remove_file(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )
        self.assertTrue(os.path.exists(os.path.join(self.repo.path, "foo")))
        cwd = os.getcwd()
        self.addCleanup(os.chdir, cwd)
        os.chdir(self.repo.path)
        porcelain.remove(self.repo.path, paths=["foo"])
        self.assertFalse(os.path.exists(os.path.join(self.repo.path, "foo")))

    def test_remove_file_staged(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        cwd = os.getcwd()
        self.addCleanup(os.chdir, cwd)
        os.chdir(self.repo.path)
        porcelain.add(self.repo.path, paths=[fullpath])
        self.assertRaises(Exception, porcelain.rm, self.repo.path, paths=["foo"])

    def test_remove_file_removed_on_disk(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        cwd = os.getcwd()
        self.addCleanup(os.chdir, cwd)
        os.chdir(self.repo.path)
        os.remove(fullpath)
        porcelain.remove(self.repo.path, paths=["foo"])
        self.assertFalse(os.path.exists(os.path.join(self.repo.path, "foo")))

    def test_remove_from_different_directory(self) -> None:
        # Create a subdirectory with a file
        subdir = os.path.join(self.repo.path, "mydir")
        os.makedirs(subdir)
        fullpath = os.path.join(subdir, "myfile")
        with open(fullpath, "w") as f:
            f.write("BAR")

        # Add and commit the file
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Change to a different directory
        cwd = os.getcwd()
        tempdir = tempfile.mkdtemp()

        def cleanup():
            os.chdir(cwd)
            shutil.rmtree(tempdir)

        self.addCleanup(cleanup)
        os.chdir(tempdir)
        # Remove the file using relative path from repository root
        porcelain.remove(self.repo.path, paths=["mydir/myfile"])

        # Verify file was removed
        self.assertFalse(os.path.exists(fullpath))

    def test_remove_with_absolute_path(self) -> None:
        # Create a file
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")

        # Add and commit the file
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Change to a different directory
        cwd = os.getcwd()
        tempdir = tempfile.mkdtemp()

        def cleanup():
            os.chdir(cwd)
            shutil.rmtree(tempdir)

        self.addCleanup(cleanup)
        os.chdir(tempdir)
        # Remove the file using absolute path
        porcelain.remove(self.repo.path, paths=[fullpath])

        # Verify file was removed
        self.assertFalse(os.path.exists(fullpath))

    def test_remove_with_filter_normalization(self) -> None:
        # Enable autocrlf to normalize line endings
        config = self.repo.get_config()
        config.set(("core",), "autocrlf", b"true")
        config.write_to_path()

        # Create a file with LF line endings (will be stored with LF in index)
        fullpath = os.path.join(self.repo.path, "foo.txt")
        with open(fullpath, "wb") as f:
            f.write(b"line1\nline2\nline3")

        # Add and commit the file (stored with LF in index)
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo,
            message=b"Add file with LF",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Simulate checkout with CRLF conversion (as would happen on Windows)
        with open(fullpath, "wb") as f:
            f.write(b"line1\r\nline2\r\nline3")

        # Verify file exists
        self.assertTrue(os.path.exists(fullpath))

        # Remove the file - this should not fail even though working tree has CRLF
        # and index has LF (thanks to the normalization in the commit)
        cwd = os.getcwd()
        os.chdir(self.repo.path)
        self.addCleanup(os.chdir, cwd)
        porcelain.remove(self.repo.path, paths=["foo.txt"])

        # Verify file was removed
        self.assertFalse(os.path.exists(fullpath))


class MvTests(PorcelainTestCase):
    def test_mv_file(self) -> None:
        # Create a file
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")

        # Add and commit the file
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Move the file
        porcelain.mv(self.repo.path, "foo", "bar")

        # Verify old path doesn't exist and new path does
        self.assertFalse(os.path.exists(os.path.join(self.repo.path, "foo")))
        self.assertTrue(os.path.exists(os.path.join(self.repo.path, "bar")))

        # Verify index was updated
        index = self.repo.open_index()
        self.assertNotIn(b"foo", index)
        self.assertIn(b"bar", index)

    def test_mv_file_to_existing_directory(self) -> None:
        # Create a file and a directory
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")

        dirpath = os.path.join(self.repo.path, "mydir")
        os.makedirs(dirpath)

        # Add and commit the file
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Move the file into the directory
        porcelain.mv(self.repo.path, "foo", "mydir")

        # Verify file moved into directory
        self.assertFalse(os.path.exists(os.path.join(self.repo.path, "foo")))
        self.assertTrue(os.path.exists(os.path.join(self.repo.path, "mydir", "foo")))

        # Verify index was updated
        index = self.repo.open_index()
        self.assertNotIn(b"foo", index)
        self.assertIn(b"mydir/foo", index)

    def test_mv_file_force_overwrite(self) -> None:
        # Create two files
        fullpath1 = os.path.join(self.repo.path, "foo")
        with open(fullpath1, "w") as f:
            f.write("FOO")

        fullpath2 = os.path.join(self.repo.path, "bar")
        with open(fullpath2, "w") as f:
            f.write("BAR")

        # Add and commit both files
        porcelain.add(self.repo.path, paths=[fullpath1, fullpath2])
        porcelain.commit(
            repo=self.repo,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Try to move without force (should fail)
        self.assertRaises(porcelain.Error, porcelain.mv, self.repo.path, "foo", "bar")

        # Move with force
        porcelain.mv(self.repo.path, "foo", "bar", force=True)

        # Verify foo doesn't exist and bar has foo's content
        self.assertFalse(os.path.exists(os.path.join(self.repo.path, "foo")))
        with open(os.path.join(self.repo.path, "bar")) as f:
            self.assertEqual(f.read(), "FOO")

    def test_mv_file_not_tracked(self) -> None:
        # Create an untracked file
        fullpath = os.path.join(self.repo.path, "untracked")
        with open(fullpath, "w") as f:
            f.write("UNTRACKED")

        # Try to move it (should fail)
        self.assertRaises(
            porcelain.Error, porcelain.mv, self.repo.path, "untracked", "tracked"
        )

    def test_mv_file_not_exists(self) -> None:
        # Try to move a non-existent file
        self.assertRaises(
            porcelain.Error, porcelain.mv, self.repo.path, "nonexistent", "destination"
        )

    def test_mv_absolute_paths(self) -> None:
        # Create a file
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")

        # Add and commit the file
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Move using absolute paths
        dest_path = os.path.join(self.repo.path, "bar")
        porcelain.mv(self.repo.path, fullpath, dest_path)

        # Verify file moved
        self.assertFalse(os.path.exists(fullpath))
        self.assertTrue(os.path.exists(dest_path))

    def test_mv_from_different_directory(self) -> None:
        # Create a subdirectory with a file
        subdir = os.path.join(self.repo.path, "mydir")
        os.makedirs(subdir)
        fullpath = os.path.join(subdir, "myfile")
        with open(fullpath, "w") as f:
            f.write("BAR")

        # Add and commit the file
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Change to a different directory and move the file
        cwd = os.getcwd()
        tempdir = tempfile.mkdtemp()

        def cleanup():
            os.chdir(cwd)
            shutil.rmtree(tempdir)

        self.addCleanup(cleanup)
        os.chdir(tempdir)
        # Move the file using relative path from repository root
        porcelain.mv(self.repo.path, "mydir/myfile", "renamed")

        # Verify file was moved
        self.assertFalse(os.path.exists(fullpath))
        self.assertTrue(os.path.exists(os.path.join(self.repo.path, "renamed")))


class LogTests(PorcelainTestCase):
    def test_simple(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        self.maxDiff = None
        outstream = StringIO()
        porcelain.log(self.repo.path, outstream=outstream)
        self.assertEqual(
            outstream.getvalue(),
            """\
--------------------------------------------------
commit: 4a3b887baa9ecb2d054d2469b628aef84e2d74f0
merge: 7508036b1cfec5aa9cef0d5a7f04abcecfe09112
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000

Commit 3

--------------------------------------------------
commit: 7508036b1cfec5aa9cef0d5a7f04abcecfe09112
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000

Commit 2

--------------------------------------------------
commit: 11d3cf672a19366435c1983c7340b008ec6b8bf3
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000

Commit 1

""",
        )

    def test_max_entries(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        outstream = StringIO()
        porcelain.log(self.repo.path, outstream=outstream, max_entries=1)
        self.assertEqual(1, outstream.getvalue().count("-" * 50))

    def test_no_revisions(self) -> None:
        outstream = StringIO()
        porcelain.log(self.repo.path, outstream=outstream)
        self.assertEqual("", outstream.getvalue())

    def test_empty_message(self) -> None:
        c1 = make_commit(message="")
        self.repo.object_store.add_object(c1)
        self.repo.refs[b"HEAD"] = c1.id
        outstream = StringIO()
        porcelain.log(self.repo.path, outstream=outstream)
        self.assertEqual(
            outstream.getvalue(),
            """\
--------------------------------------------------
commit: 4a7ad5552fad70647a81fb9a4a923ccefcca4b76
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000
""",
        )


class ShowTests(PorcelainTestCase):
    def test_nolist(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=c3.id, outstream=outstream)
        self.assertTrue(outstream.getvalue().startswith("-" * 50))

    def test_simple(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=[c3.id], outstream=outstream)
        self.assertTrue(outstream.getvalue().startswith("-" * 50))

    def test_blob(self) -> None:
        b = Blob.from_string(b"The Foo\n")
        self.repo.object_store.add_object(b)
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=[b.id], outstream=outstream)
        self.assertEqual(outstream.getvalue(), "The Foo\n")

    def test_commit_no_parent(self) -> None:
        a = Blob.from_string(b"The Foo\n")
        ta = Tree()
        ta.add(b"somename", 0o100644, a.id)
        ca = make_commit(tree=ta.id)
        self.repo.object_store.add_objects([(a, None), (ta, None), (ca, None)])
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=[ca.id], outstream=outstream)
        self.assertMultiLineEqual(
            outstream.getvalue(),
            """\
--------------------------------------------------
commit: 344da06c1bb85901270b3e8875c988a027ec087d
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000

Test message.

diff --git a/somename b/somename
new file mode 100644
index 0000000..ea5c7bf
--- /dev/null
+++ b/somename
@@ -0,0 +1 @@
+The Foo
""",
        )

    def test_tag(self) -> None:
        a = Blob.from_string(b"The Foo\n")
        ta = Tree()
        ta.add(b"somename", 0o100644, a.id)
        ca = make_commit(tree=ta.id)
        self.repo.object_store.add_objects([(a, None), (ta, None), (ca, None)])
        porcelain.tag_create(
            self.repo.path,
            b"tryme",
            b"foo <foo@bar.com>",
            b"bar",
            annotated=True,
            objectish=ca.id,
            tag_time=1552854211,
            tag_timezone=0,
        )
        outstream = StringIO()
        porcelain.show(self.repo, objects=[b"refs/tags/tryme"], outstream=outstream)
        self.maxDiff = None
        self.assertMultiLineEqual(
            outstream.getvalue(),
            """\
Tagger: foo <foo@bar.com>
Date:   Sun Mar 17 2019 20:23:31 +0000

bar

--------------------------------------------------
commit: 344da06c1bb85901270b3e8875c988a027ec087d
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000

Test message.

diff --git a/somename b/somename
new file mode 100644
index 0000000..ea5c7bf
--- /dev/null
+++ b/somename
@@ -0,0 +1 @@
+The Foo
""",
        )

    def test_tag_unicode(self) -> None:
        a = Blob.from_string(b"The Foo\n")
        ta = Tree()
        ta.add(b"somename", 0o100644, a.id)
        ca = make_commit(tree=ta.id)
        self.repo.object_store.add_objects([(a, None), (ta, None), (ca, None)])
        porcelain.tag_create(
            self.repo.path,
            "tryme",
            "foo <foo@bar.com>",
            "bar",
            annotated=True,
            objectish=ca.id,
            tag_time=1552854211,
            tag_timezone=0,
        )
        outstream = StringIO()
        porcelain.show(self.repo, objects=[b"refs/tags/tryme"], outstream=outstream)
        self.maxDiff = None
        self.assertMultiLineEqual(
            outstream.getvalue(),
            """\
Tagger: foo <foo@bar.com>
Date:   Sun Mar 17 2019 20:23:31 +0000

bar

--------------------------------------------------
commit: 344da06c1bb85901270b3e8875c988a027ec087d
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000

Test message.

diff --git a/somename b/somename
new file mode 100644
index 0000000..ea5c7bf
--- /dev/null
+++ b/somename
@@ -0,0 +1 @@
+The Foo
""",
        )

    def test_commit_with_change(self) -> None:
        a = Blob.from_string(b"The Foo\n")
        ta = Tree()
        ta.add(b"somename", 0o100644, a.id)
        ca = make_commit(tree=ta.id)
        b = Blob.from_string(b"The Bar\n")
        tb = Tree()
        tb.add(b"somename", 0o100644, b.id)
        cb = make_commit(tree=tb.id, parents=[ca.id])
        self.repo.object_store.add_objects(
            [
                (a, None),
                (b, None),
                (ta, None),
                (tb, None),
                (ca, None),
                (cb, None),
            ]
        )
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=[cb.id], outstream=outstream)
        self.assertMultiLineEqual(
            outstream.getvalue(),
            """\
--------------------------------------------------
commit: 2c6b6c9cb72c130956657e1fdae58e5b103744fa
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000

Test message.

diff --git a/somename b/somename
index ea5c7bf..fd38bcb 100644
--- a/somename
+++ b/somename
@@ -1 +1 @@
-The Foo
+The Bar
""",
        )


class SymbolicRefTests(PorcelainTestCase):
    def test_set_wrong_symbolic_ref(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        self.assertRaises(
            porcelain.Error, porcelain.symbolic_ref, self.repo.path, b"foobar"
        )

    def test_set_force_wrong_symbolic_ref(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.symbolic_ref(self.repo.path, b"force_foobar", force=True)

        # test if we actually changed the file
        with self.repo.get_named_file("HEAD") as f:
            new_ref = f.read()
        self.assertEqual(new_ref, b"ref: refs/heads/force_foobar\n")

    def test_set_symbolic_ref(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.symbolic_ref(self.repo.path, b"master")

    def test_set_symbolic_ref_other_than_master(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store,
            [[1], [2, 1], [3, 1, 2]],
            attrs=dict(refs="develop"),
        )
        self.repo.refs[b"HEAD"] = c3.id
        self.repo.refs[b"refs/heads/develop"] = c3.id

        porcelain.symbolic_ref(self.repo.path, b"develop")

        # test if we actually changed the file
        with self.repo.get_named_file("HEAD") as f:
            new_ref = f.read()
        self.assertEqual(new_ref, b"ref: refs/heads/develop\n")


class DiffTreeTests(PorcelainTestCase):
    def test_empty(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        outstream = BytesIO()
        porcelain.diff_tree(self.repo.path, c2.tree, c3.tree, outstream=outstream)
        self.assertEqual(outstream.getvalue(), b"")


class CommitTreeTests(PorcelainTestCase):
    def test_simple(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        b = Blob()
        b.data = b"foo the bar"
        t = Tree()
        t.add(b"somename", 0o100644, b.id)
        self.repo.object_store.add_object(t)
        self.repo.object_store.add_object(b)
        sha = porcelain.commit_tree(
            self.repo.path,
            t.id,
            message=b"Withcommit.",
            author=b"Joe <joe@example.com>",
            committer=b"Jane <jane@example.com>",
        )
        self.assertIsInstance(sha, bytes)
        self.assertEqual(len(sha), 40)


class RevListTests(PorcelainTestCase):
    def test_simple(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        outstream = BytesIO()
        porcelain.rev_list(self.repo.path, [c3.id], outstream=outstream)
        self.assertEqual(
            c3.id + b"\n" + c2.id + b"\n" + c1.id + b"\n", outstream.getvalue()
        )


@skipIf(
    platform.python_implementation() == "PyPy" or sys.platform == "win32",
    "gpgme not easily available or supported on Windows and PyPy",
)
class TagCreateSignTests(PorcelainGpgTestCase):
    def test_default_key(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        self.import_default_key()

        porcelain.tag_create(
            self.repo.path,
            b"tryme",
            b"foo <foo@bar.com>",
            b"bar",
            annotated=True,
            sign=True,
        )

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"tryme"])
        tag = self.repo[b"refs/tags/tryme"]
        self.assertIsInstance(tag, Tag)
        self.assertEqual(b"foo <foo@bar.com>", tag.tagger)
        self.assertEqual(b"bar\n", tag.message)
        self.assertRecentTimestamp(tag.tag_time)
        tag = self.repo[b"refs/tags/tryme"]
        assert isinstance(tag, Tag)
        # GPG Signatures aren't deterministic, so we can't do a static assertion.
        tag.verify()
        tag.verify(keyids=[PorcelainGpgTestCase.DEFAULT_KEY_ID])

        self.import_non_default_key()
        self.assertRaises(
            gpg.errors.MissingSignatures,
            tag.verify,
            keyids=[PorcelainGpgTestCase.NON_DEFAULT_KEY_ID],
        )

        assert tag.signature is not None
        tag._chunked_text = [b"bad data", tag.signature]
        self.assertRaises(
            gpg.errors.BadSignatures,
            tag.verify,
        )

    def test_non_default_key(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        self.import_non_default_key()

        porcelain.tag_create(
            self.repo.path,
            b"tryme",
            b"foo <foo@bar.com>",
            b"bar",
            annotated=True,
            sign=True,
        )

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"tryme"])
        tag = self.repo[b"refs/tags/tryme"]
        self.assertIsInstance(tag, Tag)
        self.assertEqual(b"foo <foo@bar.com>", tag.tagger)
        self.assertEqual(b"bar\n", tag.message)
        self.assertRecentTimestamp(tag.tag_time)
        tag = self.repo[b"refs/tags/tryme"]
        assert isinstance(tag, Tag)
        # GPG Signatures aren't deterministic, so we can't do a static assertion.
        tag.verify()

    def test_sign_uses_config_signingkey(self) -> None:
        """Test that sign=True uses user.signingKey from config."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up user.signingKey in config
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        cfg.write_to_path()

        self.import_default_key()

        # Create tag with sign=True (should use signingKey from config)
        porcelain.tag_create(
            self.repo.path,
            b"signed-tag",
            b"foo <foo@bar.com>",
            b"Tag with configured key",
            annotated=True,
            sign=True,  # This should read user.signingKey from config
        )

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"signed-tag"])
        tag = self.repo[b"refs/tags/signed-tag"]
        self.assertIsInstance(tag, Tag)

        # Verify the tag is signed with the configured key
        tag.verify()
        tag.verify(keyids=[PorcelainGpgTestCase.DEFAULT_KEY_ID])

    def test_tag_gpg_sign_config_enabled(self) -> None:
        """Test that tag.gpgSign=true automatically signs tags."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up user.signingKey and tag.gpgSign in config
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        cfg.set(("tag",), "gpgSign", True)
        cfg.write_to_path()

        self.import_default_key()

        # Create tag without explicit sign parameter (should auto-sign due to config)
        porcelain.tag_create(
            self.repo.path,
            b"auto-signed-tag",
            b"foo <foo@bar.com>",
            b"Auto-signed tag",
            annotated=True,
            # No sign parameter - should use tag.gpgSign config
        )

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"auto-signed-tag"])
        tag = self.repo[b"refs/tags/auto-signed-tag"]
        self.assertIsInstance(tag, Tag)

        # Verify the tag is signed due to config
        tag.verify()
        tag.verify(keyids=[PorcelainGpgTestCase.DEFAULT_KEY_ID])

    def test_tag_gpg_sign_config_disabled(self) -> None:
        """Test that tag.gpgSign=false does not sign tags."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up user.signingKey and tag.gpgSign=false in config
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        cfg.set(("tag",), "gpgSign", False)
        cfg.write_to_path()

        self.import_default_key()

        # Create tag without explicit sign parameter (should not sign)
        porcelain.tag_create(
            self.repo.path,
            b"unsigned-tag",
            b"foo <foo@bar.com>",
            b"Unsigned tag",
            annotated=True,
            # No sign parameter - should use tag.gpgSign=false config
        )

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"unsigned-tag"])
        tag = self.repo[b"refs/tags/unsigned-tag"]
        self.assertIsInstance(tag, Tag)

        # Verify the tag is not signed
        self.assertIsNone(tag._signature)

    def test_tag_gpg_sign_config_no_signing_key(self) -> None:
        """Test that tag.gpgSign=true works without user.signingKey (uses default)."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up tag.gpgSign but no user.signingKey
        cfg = self.repo.get_config()
        cfg.set(("tag",), "gpgSign", True)
        cfg.write_to_path()

        self.import_default_key()

        # Create tag without explicit sign parameter (should auto-sign with default key)
        porcelain.tag_create(
            self.repo.path,
            b"default-signed-tag",
            b"foo <foo@bar.com>",
            b"Default signed tag",
            annotated=True,
            # No sign parameter - should use tag.gpgSign config with default key
        )

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"default-signed-tag"])
        tag = self.repo[b"refs/tags/default-signed-tag"]
        self.assertIsInstance(tag, Tag)

        # Verify the tag is signed with default key
        tag.verify()

    def test_explicit_sign_overrides_config(self) -> None:
        """Test that explicit sign parameter overrides tag.gpgSign config."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up tag.gpgSign=false but explicitly pass sign=True
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        cfg.set(("tag",), "gpgSign", False)
        cfg.write_to_path()

        self.import_default_key()

        # Create tag with explicit sign=True (should override config)
        porcelain.tag_create(
            self.repo.path,
            b"explicit-signed-tag",
            b"foo <foo@bar.com>",
            b"Explicitly signed tag",
            annotated=True,
            sign=True,  # This should override tag.gpgSign=false
        )

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"explicit-signed-tag"])
        tag = self.repo[b"refs/tags/explicit-signed-tag"]
        self.assertIsInstance(tag, Tag)

        # Verify the tag is signed despite config=false
        tag.verify()
        tag.verify(keyids=[PorcelainGpgTestCase.DEFAULT_KEY_ID])

    def test_explicit_false_disables_tag_signing(self) -> None:
        """Test that explicit sign=False disables signing even with config=true."""
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        # Set up tag.gpgSign=true but explicitly pass sign=False
        cfg = self.repo.get_config()
        cfg.set(("user",), "signingKey", PorcelainGpgTestCase.DEFAULT_KEY_ID)
        cfg.set(("tag",), "gpgSign", True)
        cfg.write_to_path()

        self.import_default_key()

        # Create tag with explicit sign=False (should disable signing)
        porcelain.tag_create(
            self.repo.path,
            b"explicit-unsigned-tag",
            b"foo <foo@bar.com>",
            b"Explicitly unsigned tag",
            annotated=True,
            sign=False,  # This should override tag.gpgSign=true
        )

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"explicit-unsigned-tag"])
        tag = self.repo[b"refs/tags/explicit-unsigned-tag"]
        self.assertIsInstance(tag, Tag)

        # Verify the tag is NOT signed despite config=true
        self.assertIsNone(tag._signature)


class TagCreateTests(PorcelainTestCase):
    def test_annotated(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.tag_create(
            self.repo.path,
            b"tryme",
            b"foo <foo@bar.com>",
            b"bar",
            annotated=True,
        )

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"tryme"])
        tag = self.repo[b"refs/tags/tryme"]
        self.assertIsInstance(tag, Tag)
        self.assertEqual(b"foo <foo@bar.com>", tag.tagger)
        self.assertEqual(b"bar\n", tag.message)
        self.assertRecentTimestamp(tag.tag_time)

    def test_unannotated(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.tag_create(self.repo.path, b"tryme", annotated=False)

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"tryme"])
        self.repo[b"refs/tags/tryme"]
        self.assertEqual(list(tags.values()), [self.repo.head()])

    def test_unannotated_unicode(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.tag_create(self.repo.path, "tryme", annotated=False)

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"tryme"])
        self.repo[b"refs/tags/tryme"]
        self.assertEqual(list(tags.values()), [self.repo.head()])


class TagListTests(PorcelainTestCase):
    def test_empty(self) -> None:
        tags = porcelain.tag_list(self.repo.path)
        self.assertEqual([], tags)

    def test_simple(self) -> None:
        self.repo.refs[b"refs/tags/foo"] = b"aa" * 20
        self.repo.refs[b"refs/tags/bar/bla"] = b"bb" * 20
        tags = porcelain.tag_list(self.repo.path)

        self.assertEqual([b"bar/bla", b"foo"], tags)


class TagDeleteTests(PorcelainTestCase):
    def test_simple(self) -> None:
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.tag_create(self.repo, b"foo")
        self.assertIn(b"foo", porcelain.tag_list(self.repo))
        porcelain.tag_delete(self.repo, b"foo")
        self.assertNotIn(b"foo", porcelain.tag_list(self.repo))


class ResetTests(PorcelainTestCase):
    def test_hard_head(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Some message",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        with open(os.path.join(self.repo.path, "foo"), "wb") as f:
            f.write(b"OOH")

        porcelain.reset(self.repo, "hard", b"HEAD")

        index = self.repo.open_index()
        changes = list(
            tree_changes(
                self.repo.object_store,
                index.commit(self.repo.object_store),
                self.repo[b"HEAD"].tree,
            )
        )

        self.assertEqual([], changes)

    def test_hard_commit(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        sha = porcelain.commit(
            self.repo.path,
            message=b"Some message",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        with open(fullpath, "wb") as f:
            f.write(b"BAZ")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Some other message",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        porcelain.reset(self.repo, "hard", sha)

        index = self.repo.open_index()
        changes = list(
            tree_changes(
                self.repo.object_store,
                index.commit(self.repo.object_store),
                self.repo[sha].tree,
            )
        )

        self.assertEqual([], changes)

    def test_hard_commit_short_hash(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        sha = porcelain.commit(
            self.repo.path,
            message=b"Some message",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        with open(fullpath, "wb") as f:
            f.write(b"BAZ")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Some other message",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Test with short hash (7 characters)
        short_sha = sha[:7].decode("ascii")
        porcelain.reset(self.repo, "hard", short_sha)

        index = self.repo.open_index()
        changes = list(
            tree_changes(
                self.repo.object_store,
                index.commit(self.repo.object_store),
                self.repo[sha].tree,
            )
        )

        self.assertEqual([], changes)

    def test_hard_deletes_untracked_files(self) -> None:
        """Test that reset --hard deletes files that don't exist in target tree."""
        # Create and commit a file
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        sha1 = porcelain.commit(
            self.repo.path,
            message=b"First commit",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Create another file and commit
        fullpath2 = os.path.join(self.repo.path, "bar")
        with open(fullpath2, "w") as f:
            f.write("BAZ")
        porcelain.add(self.repo.path, paths=[fullpath2])
        porcelain.commit(
            self.repo.path,
            message=b"Second commit",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Reset hard to first commit - this should delete 'bar'
        porcelain.reset(self.repo, "hard", sha1)

        # Check that 'foo' still exists and 'bar' is deleted
        self.assertTrue(os.path.exists(fullpath))
        self.assertFalse(os.path.exists(fullpath2))

        # Check index matches first commit
        index = self.repo.open_index()
        self.assertIn(b"foo", index)
        self.assertNotIn(b"bar", index)

    def test_hard_deletes_files_in_subdirs(self) -> None:
        """Test that reset --hard deletes files in subdirectories."""
        # Create and commit files in subdirectory
        subdir = os.path.join(self.repo.path, "subdir")
        os.makedirs(subdir)
        file1 = os.path.join(subdir, "file1")
        file2 = os.path.join(subdir, "file2")

        with open(file1, "w") as f:
            f.write("content1")
        with open(file2, "w") as f:
            f.write("content2")

        porcelain.add(self.repo.path, paths=[file1, file2])
        porcelain.commit(
            self.repo.path,
            message=b"First commit",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Remove one file from subdirectory and commit
        porcelain.rm(self.repo.path, paths=[file2])
        sha2 = porcelain.commit(
            self.repo.path,
            message=b"Remove file2",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Create file2 again (untracked)
        with open(file2, "w") as f:
            f.write("new content")

        # Reset to commit that has file2 removed - should delete untracked file2
        porcelain.reset(self.repo, "hard", sha2)

        self.assertTrue(os.path.exists(file1))
        self.assertFalse(os.path.exists(file2))

    def test_hard_reset_to_remote_branch(self) -> None:
        """Test reset --hard to remote branch deletes local files not in remote."""
        # Create a file and commit
        file1 = os.path.join(self.repo.path, "file1")
        with open(file1, "w") as f:
            f.write("content1")
        porcelain.add(self.repo.path, paths=[file1])
        sha1 = porcelain.commit(
            self.repo.path,
            message=b"Initial commit",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Create a "remote" ref that doesn't have additional files
        self.repo.refs[b"refs/remotes/origin/master"] = sha1

        # Add another file locally and commit
        file2 = os.path.join(self.repo.path, "file2")
        with open(file2, "w") as f:
            f.write("content2")
        porcelain.add(self.repo.path, paths=[file2])
        porcelain.commit(
            self.repo.path,
            message=b"Add file2",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Both files should exist
        self.assertTrue(os.path.exists(file1))
        self.assertTrue(os.path.exists(file2))

        # Reset to remote branch - should delete file2
        porcelain.reset(self.repo, "hard", b"refs/remotes/origin/master")

        # file1 should exist, file2 should be deleted
        self.assertTrue(os.path.exists(file1))
        self.assertFalse(os.path.exists(file2))

    def test_mixed_reset(self) -> None:
        # Create initial commit
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        first_sha = porcelain.commit(
            self.repo.path,
            message=b"First commit",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Make second commit with modified content
        with open(fullpath, "w") as f:
            f.write("BAZ")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Second commit",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Modify working tree without staging
        with open(fullpath, "w") as f:
            f.write("MODIFIED")

        # Mixed reset to first commit
        porcelain.reset(self.repo, "mixed", first_sha)

        # Check that HEAD points to first commit
        self.assertEqual(self.repo.head(), first_sha)

        # Check that index matches first commit
        index = self.repo.open_index()
        changes = list(
            tree_changes(
                self.repo.object_store,
                index.commit(self.repo.object_store),
                self.repo[first_sha].tree,
            )
        )
        self.assertEqual([], changes)

        # Check that working tree is unchanged (still has "MODIFIED")
        with open(fullpath) as f:
            self.assertEqual(f.read(), "MODIFIED")

    def test_soft_reset(self) -> None:
        # Create initial commit
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        first_sha = porcelain.commit(
            self.repo.path,
            message=b"First commit",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Make second commit with modified content
        with open(fullpath, "w") as f:
            f.write("BAZ")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Second commit",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Stage a new change
        with open(fullpath, "w") as f:
            f.write("STAGED")
        porcelain.add(self.repo.path, paths=[fullpath])

        # Soft reset to first commit
        porcelain.reset(self.repo, "soft", first_sha)

        # Check that HEAD points to first commit
        self.assertEqual(self.repo.head(), first_sha)

        # Check that index still has the staged change (not reset)
        index = self.repo.open_index()
        # The index should still contain the staged content, not the first commit's content
        self.assertIn(b"foo", index)

        # Check that working tree is unchanged
        with open(fullpath) as f:
            self.assertEqual(f.read(), "STAGED")


class ResetFileTests(PorcelainTestCase):
    def test_reset_modify_file_to_commit(self) -> None:
        file = "foo"
        full_path = os.path.join(self.repo.path, file)

        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        sha = porcelain.commit(
            self.repo,
            message=b"unitest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        with open(full_path, "a") as f:
            f.write("something new")
        porcelain.reset_file(self.repo, file, target=sha)

        with open(full_path) as f:
            self.assertEqual("hello", f.read())

    def test_reset_remove_file_to_commit(self) -> None:
        file = "foo"
        full_path = os.path.join(self.repo.path, file)

        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        sha = porcelain.commit(
            self.repo,
            message=b"unitest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        os.remove(full_path)
        porcelain.reset_file(self.repo, file, target=sha)

        with open(full_path) as f:
            self.assertEqual("hello", f.read())

    def test_resetfile_with_dir(self) -> None:
        os.mkdir(os.path.join(self.repo.path, "new_dir"))
        full_path = os.path.join(self.repo.path, "new_dir", "foo")

        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        sha = porcelain.commit(
            self.repo,
            message=b"unitest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        with open(full_path, "a") as f:
            f.write("something new")
        porcelain.commit(
            self.repo,
            message=b"unitest 2",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        porcelain.reset_file(self.repo, os.path.join("new_dir", "foo"), target=sha)

        with open(full_path) as f:
            self.assertEqual("hello", f.read())


def _commit_file_with_content(repo, filename, content):
    file_path = os.path.join(repo.path, filename)

    with open(file_path, "w") as f:
        f.write(content)
    porcelain.add(repo, paths=[file_path])
    sha = porcelain.commit(
        repo,
        message=b"add " + filename.encode(),
        committer=b"Jane <jane@example.com>",
        author=b"John <john@example.com>",
    )

    return sha, file_path


class RevertTests(PorcelainTestCase):
    def test_revert_simple(self) -> None:
        # Create initial commit
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("initial content\n")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Initial commit",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Make a change
        with open(fullpath, "w") as f:
            f.write("modified content\n")
        porcelain.add(self.repo.path, paths=[fullpath])
        change_sha = porcelain.commit(
            self.repo.path,
            message=b"Change content",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Revert the change
        revert_sha = porcelain.revert(self.repo.path, commits=[change_sha])

        # Check the file content is back to initial
        with open(fullpath) as f:
            self.assertEqual("initial content\n", f.read())

        # Check the revert commit message
        revert_commit = self.repo[revert_sha]
        self.assertIn(b'Revert "Change content"', revert_commit.message)
        self.assertIn(change_sha[:7], revert_commit.message)

    def test_revert_multiple(self) -> None:
        # Create initial commit
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("line1\n")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Initial commit",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Add line2
        with open(fullpath, "a") as f:
            f.write("line2\n")
        porcelain.add(self.repo.path, paths=[fullpath])
        commit1 = porcelain.commit(
            self.repo.path,
            message=b"Add line2",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Add line3
        with open(fullpath, "a") as f:
            f.write("line3\n")
        porcelain.add(self.repo.path, paths=[fullpath])
        commit2 = porcelain.commit(
            self.repo.path,
            message=b"Add line3",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Revert both commits (in reverse order)
        porcelain.revert(self.repo.path, commits=[commit2, commit1])

        # Check file is back to initial state
        with open(fullpath) as f:
            self.assertEqual("line1\n", f.read())

    def test_revert_no_commit(self) -> None:
        # Create initial commit
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("initial\n")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Initial",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Make a change
        with open(fullpath, "w") as f:
            f.write("changed\n")
        porcelain.add(self.repo.path, paths=[fullpath])
        change_sha = porcelain.commit(
            self.repo.path,
            message=b"Change",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Revert with no_commit
        result = porcelain.revert(self.repo.path, commits=[change_sha], no_commit=True)

        # Should return None
        self.assertIsNone(result)

        # File should be reverted
        with open(fullpath) as f:
            self.assertEqual("initial\n", f.read())

        # HEAD should still point to the change commit
        self.assertEqual(self.repo.refs[b"HEAD"], change_sha)

    def test_revert_custom_message(self) -> None:
        # Create commits
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("initial\n")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Initial",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        with open(fullpath, "w") as f:
            f.write("changed\n")
        porcelain.add(self.repo.path, paths=[fullpath])
        change_sha = porcelain.commit(
            self.repo.path,
            message=b"Change",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Revert with custom message
        custom_msg = "Custom revert message"
        revert_sha = porcelain.revert(
            self.repo.path, commits=[change_sha], message=custom_msg
        )

        # Check the message
        revert_commit = self.repo[revert_sha]
        self.assertEqual(custom_msg.encode("utf-8"), revert_commit.message)

    def test_revert_no_parent(self) -> None:
        # Try to revert the initial commit (no parent)
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("content\n")
        porcelain.add(self.repo.path, paths=[fullpath])
        initial_sha = porcelain.commit(
            self.repo.path,
            message=b"Initial",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

        # Should raise an error
        with self.assertRaises(porcelain.Error) as cm:
            porcelain.revert(self.repo.path, commits=[initial_sha])
        self.assertIn("no parents", str(cm.exception))


class CheckoutTests(PorcelainTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._sha, self._foo_path = _commit_file_with_content(
            self.repo, "foo", "hello\n"
        )
        porcelain.branch_create(self.repo, "uni")

    def test_checkout_to_existing_branch(self) -> None:
        self.assertEqual(b"master", porcelain.active_branch(self.repo))
        porcelain.checkout(self.repo, b"uni")
        self.assertEqual(b"uni", porcelain.active_branch(self.repo))

    def test_checkout_to_non_existing_branch(self) -> None:
        self.assertEqual(b"master", porcelain.active_branch(self.repo))

        with self.assertRaises(KeyError):
            porcelain.checkout(self.repo, b"bob")

        self.assertEqual(b"master", porcelain.active_branch(self.repo))

    def test_checkout_to_branch_with_modified_files(self) -> None:
        with open(self._foo_path, "a") as f:
            f.write("new message\n")
        porcelain.add(self.repo, paths=[self._foo_path])

        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [], "delete": [], "modify": [b"foo"]}, [], []], status
        )

        # The new checkout behavior prevents switching with staged changes
        with self.assertRaises(porcelain.CheckoutError):
            porcelain.checkout(self.repo, b"uni")

        # Should still be on master
        self.assertEqual(b"master", porcelain.active_branch(self.repo))

        # Force checkout should work
        porcelain.checkout(self.repo, b"uni", force=True)
        self.assertEqual(b"uni", porcelain.active_branch(self.repo))

    def test_checkout_with_deleted_files(self) -> None:
        porcelain.remove(self.repo.path, [os.path.join(self.repo.path, "foo")])
        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [], "delete": [b"foo"], "modify": []}, [], []], status
        )

        # The new checkout behavior prevents switching with staged deletions
        with self.assertRaises(porcelain.CheckoutError):
            porcelain.checkout(self.repo, b"uni")

        # Should still be on master
        self.assertEqual(b"master", porcelain.active_branch(self.repo))

        # Force checkout should work
        porcelain.checkout(self.repo, b"uni", force=True)
        self.assertEqual(b"uni", porcelain.active_branch(self.repo))

    def test_checkout_to_branch_with_added_files(self) -> None:
        file_path = os.path.join(self.repo.path, "bar")

        with open(file_path, "w") as f:
            f.write("bar content\n")
        porcelain.add(self.repo, paths=[file_path])
        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [b"bar"], "delete": [], "modify": []}, [], []], status
        )

        # Both branches have file 'foo' checkout should be fine.
        porcelain.checkout(self.repo, b"uni")
        self.assertEqual(b"uni", porcelain.active_branch(self.repo))

        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [b"bar"], "delete": [], "modify": []}, [], []], status
        )

    def test_checkout_to_branch_with_modified_file_not_present(self) -> None:
        # Commit a new file that the other branch doesn't have.
        _, nee_path = _commit_file_with_content(self.repo, "nee", "Good content\n")

        # Modify the file the other branch doesn't have.
        with open(nee_path, "a") as f:
            f.write("bar content\n")
        porcelain.add(self.repo, paths=[nee_path])
        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [], "delete": [], "modify": [b"nee"]}, [], []], status
        )

        # The new checkout behavior allows switching if the file doesn't exist in target branch
        # (changes can be preserved)
        porcelain.checkout(self.repo, b"uni")
        self.assertEqual(b"uni", porcelain.active_branch(self.repo))

        # The staged changes are lost and the file is removed from working tree
        # because it doesn't exist in the target branch
        status = list(porcelain.status(self.repo))
        # File 'nee' is gone completely
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], []], status)
        self.assertFalse(os.path.exists(nee_path))

    def test_checkout_to_branch_with_modified_file_not_present_forced(self) -> None:
        # Commit a new file that the other branch doesn't have.
        _, nee_path = _commit_file_with_content(self.repo, "nee", "Good content\n")

        # Modify the file the other branch doesn't have.
        with open(nee_path, "a") as f:
            f.write("bar content\n")
        porcelain.add(self.repo, paths=[nee_path])
        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [], "delete": [], "modify": [b"nee"]}, [], []], status
        )

        # 'uni' branch doesn't have 'nee' and it has been modified, but we force to reset the entire index.
        porcelain.checkout(self.repo, b"uni", force=True)

        self.assertEqual(b"uni", porcelain.active_branch(self.repo))

        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], []], status)

    def test_checkout_to_branch_with_unstaged_files(self) -> None:
        # Edit `foo`.
        with open(self._foo_path, "a") as f:
            f.write("new message")

        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [], "delete": [], "modify": []}, [b"foo"], []], status
        )

        # The new checkout behavior prevents switching with unstaged changes
        with self.assertRaises(porcelain.CheckoutError):
            porcelain.checkout(self.repo, b"uni")

        # Should still be on master
        self.assertEqual(b"master", porcelain.active_branch(self.repo))

        # Force checkout should work
        porcelain.checkout(self.repo, b"uni", force=True)
        self.assertEqual(b"uni", porcelain.active_branch(self.repo))

    def test_checkout_to_branch_with_untracked_files(self) -> None:
        with open(os.path.join(self.repo.path, "neu"), "a") as f:
            f.write("new message\n")

        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], ["neu"]], status)

        porcelain.checkout(self.repo, b"uni")

        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], ["neu"]], status)

    def test_checkout_to_branch_with_new_files(self) -> None:
        porcelain.checkout(self.repo, b"uni")
        sub_directory = os.path.join(self.repo.path, "sub1")
        os.mkdir(sub_directory)
        for index in range(5):
            _commit_file_with_content(
                self.repo, "new_file_" + str(index + 1), "Some content\n"
            )
            _commit_file_with_content(
                self.repo,
                os.path.join("sub1", "new_file_" + str(index + 10)),
                "Good content\n",
            )

        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], []], status)

        porcelain.checkout(self.repo, b"master")
        self.assertEqual(b"master", porcelain.active_branch(self.repo))
        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], []], status)

        porcelain.checkout(self.repo, b"uni")
        self.assertEqual(b"uni", porcelain.active_branch(self.repo))
        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], []], status)

    def test_checkout_to_branch_with_file_in_sub_directory(self) -> None:
        sub_directory = os.path.join(self.repo.path, "sub1", "sub2")
        os.makedirs(sub_directory)

        sub_directory_file = os.path.join(sub_directory, "neu")
        with open(sub_directory_file, "w") as f:
            f.write("new message\n")

        porcelain.add(self.repo, paths=[sub_directory_file])
        porcelain.commit(
            self.repo,
            message=b"add " + sub_directory_file.encode(),
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], []], status)

        self.assertTrue(os.path.isdir(sub_directory))
        self.assertTrue(os.path.isdir(os.path.dirname(sub_directory)))

        porcelain.checkout(self.repo, b"uni")

        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], []], status)

        self.assertFalse(os.path.isdir(sub_directory))
        self.assertFalse(os.path.isdir(os.path.dirname(sub_directory)))

        porcelain.checkout(self.repo, b"master")

        self.assertTrue(os.path.isdir(sub_directory))
        self.assertTrue(os.path.isdir(os.path.dirname(sub_directory)))

    def test_checkout_to_branch_with_multiple_files_in_sub_directory(self) -> None:
        sub_directory = os.path.join(self.repo.path, "sub1", "sub2")
        os.makedirs(sub_directory)

        sub_directory_file_1 = os.path.join(sub_directory, "neu")
        with open(sub_directory_file_1, "w") as f:
            f.write("new message\n")

        sub_directory_file_2 = os.path.join(sub_directory, "gus")
        with open(sub_directory_file_2, "w") as f:
            f.write("alternative message\n")

        porcelain.add(self.repo, paths=[sub_directory_file_1, sub_directory_file_2])
        porcelain.commit(
            self.repo,
            message=b"add files neu and gus.",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], []], status)

        self.assertTrue(os.path.isdir(sub_directory))
        self.assertTrue(os.path.isdir(os.path.dirname(sub_directory)))

        porcelain.checkout(self.repo, b"uni")

        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], []], status)

        self.assertFalse(os.path.isdir(sub_directory))
        self.assertFalse(os.path.isdir(os.path.dirname(sub_directory)))

    def _commit_something_wrong(self):
        with open(self._foo_path, "a") as f:
            f.write("something wrong")

        porcelain.add(self.repo, paths=[self._foo_path])
        return porcelain.commit(
            self.repo,
            message=b"I may added something wrong",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )

    def test_checkout_to_commit_sha(self) -> None:
        self._commit_something_wrong()

        porcelain.checkout(self.repo, self._sha)
        self.assertEqual(self._sha, self.repo.head())

    def test_checkout_to_head(self) -> None:
        new_sha = self._commit_something_wrong()

        porcelain.checkout(self.repo, b"HEAD")
        self.assertEqual(new_sha, self.repo.head())

    def _checkout_remote_branch(self):
        errstream = BytesIO()
        outstream = BytesIO()

        porcelain.commit(
            repo=self.repo.path,
            message=b"init",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.clone(
            self.repo.path, target=clone_path, errstream=errstream
        )
        self.addCleanup(target_repo.close)
        self.assertEqual(target_repo[b"HEAD"], self.repo[b"HEAD"])

        # create a second file to be pushed back to origin
        handle, fullpath = tempfile.mkstemp(dir=clone_path)
        os.close(handle)
        porcelain.add(repo=clone_path, paths=[fullpath])
        porcelain.commit(
            repo=clone_path,
            message=b"push",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Setup a non-checked out branch in the remote
        refs_path = b"refs/heads/foo"
        new_id = self.repo[b"HEAD"].id
        self.assertNotEqual(new_id, ZERO_SHA)
        self.repo.refs[refs_path] = new_id

        # Push to the remote
        porcelain.push(
            clone_path,
            "origin",
            b"HEAD:" + refs_path,
            outstream=outstream,
            errstream=errstream,
        )

        self.assertEqual(
            target_repo.refs[b"refs/remotes/origin/foo"],
            target_repo.refs[b"HEAD"],
        )

        # The new checkout behavior treats origin/foo as a ref and creates detached HEAD
        porcelain.checkout(target_repo, b"origin/foo")
        original_id = target_repo[b"HEAD"].id
        uni_id = target_repo[b"refs/remotes/origin/uni"].id

        # Should be in detached HEAD state
        with self.assertRaises((ValueError, IndexError)):
            porcelain.active_branch(target_repo)

        expected_refs = {
            b"HEAD": original_id,
            b"refs/heads/master": original_id,
            # No local foo branch is created anymore
            b"refs/remotes/origin/foo": original_id,
            b"refs/remotes/origin/uni": uni_id,
            b"refs/remotes/origin/HEAD": new_id,
            b"refs/remotes/origin/master": new_id,
        }
        self.assertEqual(expected_refs, target_repo.get_refs())

        return target_repo

    def test_checkout_remote_branch(self) -> None:
        repo = self._checkout_remote_branch()
        repo.close()

    def test_checkout_remote_branch_then_master_then_remote_branch_again(self) -> None:
        target_repo = self._checkout_remote_branch()
        # Should be in detached HEAD state
        with self.assertRaises((ValueError, IndexError)):
            porcelain.active_branch(target_repo)

        # Save the commit SHA before adding bar
        detached_commit_sha, _ = _commit_file_with_content(
            target_repo, "bar", "something\n"
        )
        self.assertTrue(os.path.isfile(os.path.join(target_repo.path, "bar")))

        porcelain.checkout(target_repo, b"master")

        self.assertEqual(b"master", porcelain.active_branch(target_repo))
        self.assertFalse(os.path.isfile(os.path.join(target_repo.path, "bar")))

        # Going back to origin/foo won't have bar because the commit was made in detached state
        porcelain.checkout(target_repo, b"origin/foo")

        # Should be in detached HEAD state again
        with self.assertRaises((ValueError, IndexError)):
            porcelain.active_branch(target_repo)
        # bar is NOT there because we're back at the original origin/foo commit
        self.assertFalse(os.path.isfile(os.path.join(target_repo.path, "bar")))

        # But we can checkout the specific commit to get bar back
        porcelain.checkout(target_repo, detached_commit_sha.decode())
        self.assertTrue(os.path.isfile(os.path.join(target_repo.path, "bar")))

        target_repo.close()

    def test_checkout_new_branch_from_remote_sets_tracking(self) -> None:
        # Create a "remote" repository
        remote_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, remote_path)
        remote_repo = porcelain.init(remote_path)

        # Add a commit to the remote
        remote_sha, _ = _commit_file_with_content(
            remote_repo, "bar", "remote content\n"
        )

        # Clone the remote repository
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        target_repo = porcelain.clone(remote_path, target_path)

        # Create a remote tracking branch reference
        remote_branch_ref = b"refs/remotes/origin/feature"
        target_repo.refs[remote_branch_ref] = remote_sha

        # Checkout a new branch from the remote branch
        porcelain.checkout(target_repo, remote_branch_ref, new_branch=b"local-feature")

        # Verify the branch was created and is active
        self.assertEqual(b"local-feature", porcelain.active_branch(target_repo))

        # Verify tracking configuration was set
        config = target_repo.get_config()
        self.assertEqual(
            b"origin", config.get((b"branch", b"local-feature"), b"remote")
        )
        self.assertEqual(
            b"refs/heads/feature", config.get((b"branch", b"local-feature"), b"merge")
        )

        target_repo.close()
        remote_repo.close()


class GeneralCheckoutTests(PorcelainTestCase):
    """Tests for the general checkout function that handles branches, tags, and commits."""

    def setUp(self) -> None:
        super().setUp()
        # Create initial commit
        self._sha1, self._foo_path = _commit_file_with_content(
            self.repo, "foo", "initial content\n"
        )
        # Create a branch
        porcelain.branch_create(self.repo, "feature")
        # Create another commit on master
        self._sha2, self._bar_path = _commit_file_with_content(
            self.repo, "bar", "bar content\n"
        )
        # Create a tag
        porcelain.tag_create(self.repo, "v1.0", objectish=self._sha1)

    def test_checkout_branch(self) -> None:
        """Test checking out a branch."""
        self.assertEqual(b"master", porcelain.active_branch(self.repo))

        # Checkout feature branch
        porcelain.checkout(self.repo, "feature")
        self.assertEqual(b"feature", porcelain.active_branch(self.repo))

        # File 'bar' should not exist in feature branch
        self.assertFalse(os.path.exists(self._bar_path))

        # Go back to master
        porcelain.checkout(self.repo, "master")
        self.assertEqual(b"master", porcelain.active_branch(self.repo))

        # File 'bar' should exist again
        self.assertTrue(os.path.exists(self._bar_path))

    def test_checkout_commit(self) -> None:
        """Test checking out a specific commit (detached HEAD)."""
        # Checkout first commit by SHA
        porcelain.checkout(self.repo, self._sha1.decode("ascii"))

        # Should be in detached HEAD state - active_branch raises IndexError
        with self.assertRaises((ValueError, IndexError)):
            porcelain.active_branch(self.repo)

        # File 'bar' should not exist
        self.assertFalse(os.path.exists(self._bar_path))

        # HEAD should point to the commit
        self.assertEqual(self._sha1, self.repo.refs[b"HEAD"])

    def test_checkout_tag(self) -> None:
        """Test checking out a tag (detached HEAD)."""
        # Checkout tag
        porcelain.checkout(self.repo, "v1.0")

        # Should be in detached HEAD state - active_branch raises IndexError
        with self.assertRaises((ValueError, IndexError)):
            porcelain.active_branch(self.repo)

        # File 'bar' should not exist (tag points to first commit)
        self.assertFalse(os.path.exists(self._bar_path))

        # HEAD should point to the tagged commit
        self.assertEqual(self._sha1, self.repo.refs[b"HEAD"])

    def test_checkout_new_branch(self) -> None:
        """Test creating a new branch during checkout (like git checkout -b)."""
        # Create and checkout new branch from current HEAD
        porcelain.checkout(self.repo, "master", new_branch="new-feature")

        self.assertEqual(b"new-feature", porcelain.active_branch(self.repo))
        self.assertTrue(os.path.exists(self._bar_path))

        # Create and checkout new branch from specific commit
        porcelain.checkout(self.repo, self._sha1.decode("ascii"), new_branch="from-old")

        self.assertEqual(b"from-old", porcelain.active_branch(self.repo))
        self.assertFalse(os.path.exists(self._bar_path))

    def test_checkout_with_uncommitted_changes(self) -> None:
        """Test checkout behavior with uncommitted changes."""
        # Modify a file
        with open(self._foo_path, "w") as f:
            f.write("modified content\n")

        # Should raise error when trying to checkout
        with self.assertRaises(porcelain.CheckoutError) as cm:
            porcelain.checkout(self.repo, "feature")

        self.assertIn("local changes", str(cm.exception))
        self.assertIn("foo", str(cm.exception))

        # Should still be on master
        self.assertEqual(b"master", porcelain.active_branch(self.repo))

    def test_checkout_force(self) -> None:
        """Test forced checkout discards local changes."""
        # Modify a file
        with open(self._foo_path, "w") as f:
            f.write("modified content\n")

        # Force checkout should succeed
        porcelain.checkout(self.repo, "feature", force=True)

        self.assertEqual(b"feature", porcelain.active_branch(self.repo))

        # Local changes should be discarded
        with open(self._foo_path) as f:
            content = f.read()
        self.assertEqual("initial content\n", content)

    def test_checkout_nonexistent_ref(self) -> None:
        """Test checkout of non-existent branch/commit."""
        with self.assertRaises(KeyError):
            porcelain.checkout(self.repo, "nonexistent")

    def test_checkout_partial_sha(self) -> None:
        """Test checkout with partial SHA."""
        # Git typically allows checkout with partial SHA
        partial_sha = self._sha1.decode("ascii")[:7]
        porcelain.checkout(self.repo, partial_sha)

        # Should be in detached HEAD state at the right commit
        self.assertEqual(self._sha1, self.repo.refs[b"HEAD"])

    def test_checkout_preserves_untracked_files(self) -> None:
        """Test that checkout preserves untracked files."""
        # Create an untracked file
        untracked_path = os.path.join(self.repo.path, "untracked.txt")
        with open(untracked_path, "w") as f:
            f.write("untracked content\n")

        # Checkout another branch
        porcelain.checkout(self.repo, "feature")

        # Untracked file should still exist
        self.assertTrue(os.path.exists(untracked_path))
        with open(untracked_path) as f:
            content = f.read()
        self.assertEqual("untracked content\n", content)

    def test_checkout_full_ref_paths(self) -> None:
        """Test checkout with full ref paths."""
        # Test checkout with full branch ref path
        porcelain.checkout(self.repo, "refs/heads/feature")
        self.assertEqual(b"feature", porcelain.active_branch(self.repo))

        # Test checkout with full tag ref path
        porcelain.checkout(self.repo, "refs/tags/v1.0")
        # Should be in detached HEAD state
        with self.assertRaises((ValueError, IndexError)):
            porcelain.active_branch(self.repo)
        self.assertEqual(self._sha1, self.repo.refs[b"HEAD"])

    def test_checkout_bytes_vs_string_target(self) -> None:
        """Test that checkout works with both bytes and string targets."""
        # Test with string target
        porcelain.checkout(self.repo, "feature")
        self.assertEqual(b"feature", porcelain.active_branch(self.repo))

        # Test with bytes target
        porcelain.checkout(self.repo, b"master")
        self.assertEqual(b"master", porcelain.active_branch(self.repo))

    def test_checkout_new_branch_from_commit(self) -> None:
        """Test creating a new branch from a specific commit."""
        # Create new branch from first commit
        porcelain.checkout(self.repo, self._sha1.decode(), new_branch="from-commit")

        self.assertEqual(b"from-commit", porcelain.active_branch(self.repo))
        # Should be at the first commit (no bar file)
        self.assertFalse(os.path.exists(self._bar_path))

    def test_checkout_with_staged_addition(self) -> None:
        """Test checkout behavior with staged file additions."""
        # Create and stage a new file that doesn't exist in target branch
        new_file_path = os.path.join(self.repo.path, "new.txt")
        with open(new_file_path, "w") as f:
            f.write("new file content\n")
        porcelain.add(self.repo, [new_file_path])

        # This should succeed because the file doesn't exist in target branch
        porcelain.checkout(self.repo, "feature")

        # Should be on feature branch
        self.assertEqual(b"feature", porcelain.active_branch(self.repo))

        # The new file should still exist and be staged
        self.assertTrue(os.path.exists(new_file_path))
        status = porcelain.status(self.repo)
        self.assertIn(b"new.txt", status.staged["add"])

    def test_checkout_with_staged_modification_conflict(self) -> None:
        """Test checkout behavior with staged modifications that would conflict."""
        # Stage changes to a file that exists in both branches
        with open(self._foo_path, "w") as f:
            f.write("modified content\n")
        porcelain.add(self.repo, [self._foo_path])

        # Should prevent checkout due to staged changes to existing file
        with self.assertRaises(porcelain.CheckoutError) as cm:
            porcelain.checkout(self.repo, "feature")

        self.assertIn("local changes", str(cm.exception))
        self.assertIn("foo", str(cm.exception))

    def test_checkout_head_reference(self) -> None:
        """Test checkout of HEAD reference."""
        # Move to feature branch first
        porcelain.checkout(self.repo, "feature")

        # Checkout HEAD creates detached HEAD state
        porcelain.checkout(self.repo, "HEAD")

        # Should be in detached HEAD state
        with self.assertRaises((ValueError, IndexError)):
            porcelain.active_branch(self.repo)

    def test_checkout_error_messages(self) -> None:
        """Test that checkout error messages are helpful."""
        # Create uncommitted changes
        with open(self._foo_path, "w") as f:
            f.write("uncommitted changes\n")

        # Try to checkout
        with self.assertRaises(porcelain.CheckoutError) as cm:
            porcelain.checkout(self.repo, "feature")

        error_msg = str(cm.exception)
        self.assertIn("local changes", error_msg)
        self.assertIn("foo", error_msg)
        self.assertIn("overwritten", error_msg)
        self.assertIn("commit or stash", error_msg)


class SubmoduleTests(PorcelainTestCase):
    def test_empty(self) -> None:
        porcelain.commit(
            repo=self.repo.path,
            message=b"init",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        self.assertEqual([], list(porcelain.submodule_list(self.repo)))

    def test_add(self) -> None:
        porcelain.submodule_add(self.repo, "../bar.git", "bar")
        with open(f"{self.repo.path}/.gitmodules") as f:
            self.assertEqual(
                """\
[submodule "bar"]
\turl = ../bar.git
\tpath = bar
""",
                f.read(),
            )

    def test_init(self) -> None:
        porcelain.submodule_add(self.repo, "../bar.git", "bar")
        porcelain.submodule_init(self.repo)

    def test_update(self) -> None:
        # Create a submodule repository
        sub_repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, sub_repo_path)
        sub_repo = Repo.init(sub_repo_path)
        self.addCleanup(sub_repo.close)

        # Add a file to the submodule repo
        sub_file = os.path.join(sub_repo_path, "test.txt")
        with open(sub_file, "w") as f:
            f.write("submodule content")

        porcelain.add(sub_repo, paths=[sub_file])
        sub_commit = porcelain.commit(
            sub_repo,
            message=b"Initial submodule commit",
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
        )

        # Add the submodule to the main repository
        porcelain.submodule_add(self.repo, sub_repo_path, "test_submodule")

        # Manually add the submodule to the index
        from dulwich.index import IndexEntry
        from dulwich.objects import S_IFGITLINK

        index = self.repo.open_index()
        index[b"test_submodule"] = IndexEntry(
            ctime=0,
            mtime=0,
            dev=0,
            ino=0,
            mode=S_IFGITLINK,
            uid=0,
            gid=0,
            size=0,
            sha=sub_commit,
            flags=0,
        )
        index.write()

        porcelain.add(self.repo, paths=[".gitmodules"])
        porcelain.commit(
            self.repo,
            message=b"Add submodule",
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
        )

        # Initialize and update the submodule
        porcelain.submodule_init(self.repo)
        porcelain.submodule_update(self.repo)

        # Check that the submodule directory exists
        submodule_path = os.path.join(self.repo.path, "test_submodule")
        self.assertTrue(os.path.exists(submodule_path))

        # Check that the submodule file exists
        submodule_file = os.path.join(submodule_path, "test.txt")
        self.assertTrue(os.path.exists(submodule_file))
        with open(submodule_file) as f:
            self.assertEqual(f.read(), "submodule content")


class PushTests(PorcelainTestCase):
    def test_simple(self) -> None:
        """Basic test of porcelain push where self.repo is the remote.  First
        clone the remote, commit a file to the clone, then push the changes
        back to the remote.
        """
        outstream = BytesIO()
        errstream = BytesIO()

        porcelain.commit(
            repo=self.repo.path,
            message=b"init",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.clone(
            self.repo.path, target=clone_path, errstream=errstream
        )
        self.addCleanup(target_repo.close)
        self.assertEqual(target_repo[b"HEAD"], self.repo[b"HEAD"])

        # create a second file to be pushed back to origin
        handle, fullpath = tempfile.mkstemp(dir=clone_path)
        os.close(handle)
        porcelain.add(repo=clone_path, paths=[fullpath])
        porcelain.commit(
            repo=clone_path,
            message=b"push",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Setup a non-checked out branch in the remote
        refs_path = b"refs/heads/foo"
        new_id = self.repo[b"HEAD"].id
        self.assertNotEqual(new_id, ZERO_SHA)
        self.repo.refs[refs_path] = new_id

        # Push to the remote
        porcelain.push(
            clone_path,
            "origin",
            b"HEAD:" + refs_path,
            outstream=outstream,
            errstream=errstream,
        )

        self.assertEqual(
            target_repo.refs[b"refs/remotes/origin/foo"],
            target_repo.refs[b"HEAD"],
        )

        # Check that the target and source
        with Repo(clone_path) as r_clone:
            self.assertEqual(
                {
                    b"HEAD": new_id,
                    b"refs/heads/foo": r_clone[b"HEAD"].id,
                    b"refs/heads/master": new_id,
                },
                self.repo.get_refs(),
            )
            self.assertEqual(r_clone[b"HEAD"].id, self.repo[refs_path].id)

            # Get the change in the target repo corresponding to the add
            # this will be in the foo branch.
            change = next(
                iter(
                    tree_changes(
                        self.repo.object_store,
                        self.repo[b"HEAD"].tree,
                        self.repo[b"refs/heads/foo"].tree,
                    )
                )
            )
            self.assertEqual(
                os.path.basename(fullpath), change.new.path.decode("ascii")
            )

    def test_local_missing(self) -> None:
        """Pushing a new branch."""
        outstream = BytesIO()
        errstream = BytesIO()

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.init(clone_path)
        target_repo.close()

        self.assertRaises(
            porcelain.Error,
            porcelain.push,
            self.repo,
            clone_path,
            b"HEAD:refs/heads/master",
            outstream=outstream,
            errstream=errstream,
        )

    def test_new(self) -> None:
        """Pushing a new branch."""
        outstream = BytesIO()
        errstream = BytesIO()

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.init(clone_path)
        target_repo.close()

        # create a second file to be pushed back to origin
        handle, fullpath = tempfile.mkstemp(dir=clone_path)
        os.close(handle)
        porcelain.add(repo=clone_path, paths=[fullpath])
        new_id = porcelain.commit(
            repo=self.repo,
            message=b"push",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Push to the remote
        porcelain.push(
            self.repo,
            clone_path,
            b"HEAD:refs/heads/master",
            outstream=outstream,
            errstream=errstream,
        )

        with Repo(clone_path) as r_clone:
            self.assertEqual(
                {
                    b"HEAD": new_id,
                    b"refs/heads/master": new_id,
                },
                r_clone.get_refs(),
            )

    def test_delete(self) -> None:
        """Basic test of porcelain push, removing a branch."""
        outstream = BytesIO()
        errstream = BytesIO()

        porcelain.commit(
            repo=self.repo.path,
            message=b"init",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.clone(
            self.repo.path, target=clone_path, errstream=errstream
        )
        target_repo.close()

        # Setup a non-checked out branch in the remote
        refs_path = b"refs/heads/foo"
        new_id = self.repo[b"HEAD"].id
        self.assertNotEqual(new_id, ZERO_SHA)
        self.repo.refs[refs_path] = new_id

        # Push to the remote
        porcelain.push(
            clone_path,
            self.repo.path,
            b":" + refs_path,
            outstream=outstream,
            errstream=errstream,
        )

        self.assertEqual(
            {
                b"HEAD": new_id,
                b"refs/heads/master": new_id,
            },
            self.repo.get_refs(),
        )

    def test_diverged(self) -> None:
        outstream = BytesIO()
        errstream = BytesIO()

        porcelain.commit(
            repo=self.repo.path,
            message=b"init",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.clone(
            self.repo.path, target=clone_path, errstream=errstream
        )
        target_repo.close()

        remote_id = porcelain.commit(
            repo=self.repo.path,
            message=b"remote change",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        local_id = porcelain.commit(
            repo=clone_path,
            message=b"local change",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        outstream = BytesIO()
        errstream = BytesIO()

        # Push to the remote
        self.assertRaises(
            porcelain.DivergedBranches,
            porcelain.push,
            clone_path,
            self.repo.path,
            b"refs/heads/master",
            outstream=outstream,
            errstream=errstream,
        )

        self.assertEqual(
            {
                b"HEAD": remote_id,
                b"refs/heads/master": remote_id,
            },
            self.repo.get_refs(),
        )

        self.assertEqual(b"", outstream.getvalue())
        self.assertEqual(b"", errstream.getvalue())

        outstream = BytesIO()
        errstream = BytesIO()

        # Push to the remote with --force
        porcelain.push(
            clone_path,
            self.repo.path,
            b"refs/heads/master",
            outstream=outstream,
            errstream=errstream,
            force=True,
        )

        self.assertEqual(
            {
                b"HEAD": local_id,
                b"refs/heads/master": local_id,
            },
            self.repo.get_refs(),
        )

        self.assertEqual(b"", outstream.getvalue())
        self.assertTrue(re.match(b"Push to .* successful.\n", errstream.getvalue()))

    def test_push_returns_sendpackresult(self) -> None:
        """Test that push returns a SendPackResult with per-ref information."""
        outstream = BytesIO()
        errstream = BytesIO()

        # Create initial commit
        porcelain.commit(
            repo=self.repo.path,
            message=b"init",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.clone(
            self.repo.path, target=clone_path, errstream=errstream
        )
        target_repo.close()

        # Create a commit in the clone
        handle, fullpath = tempfile.mkstemp(dir=clone_path)
        os.close(handle)
        porcelain.add(repo=clone_path, paths=[fullpath])
        porcelain.commit(
            repo=clone_path,
            message=b"push",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Push and check the return value
        result = porcelain.push(
            clone_path,
            "origin",
            b"HEAD:refs/heads/new-branch",
            outstream=outstream,
            errstream=errstream,
        )

        # Verify that we get a SendPackResult
        self.assertIsInstance(result, SendPackResult)

        # Verify that it contains refs
        self.assertIsNotNone(result.refs)
        self.assertIn(b"refs/heads/new-branch", result.refs)

        # Verify ref_status - should be None for successful updates
        if result.ref_status:
            self.assertIsNone(result.ref_status.get(b"refs/heads/new-branch"))

    def test_mirror_mode(self) -> None:
        """Test push with remote.<name>.mirror configuration."""
        outstream = BytesIO()
        errstream = BytesIO()

        # Create initial commit
        porcelain.commit(
            repo=self.repo.path,
            message=b"init",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.clone(
            self.repo.path, target=clone_path, errstream=errstream
        )
        target_repo.close()

        # Create multiple refs in the clone
        with Repo(clone_path) as r_clone:
            # Create a new branch
            r_clone.refs[b"refs/heads/feature"] = r_clone[b"HEAD"].id
            # Create a tag
            r_clone.refs[b"refs/tags/v1.0"] = r_clone[b"HEAD"].id
            # Create a remote tracking branch
            r_clone.refs[b"refs/remotes/upstream/main"] = r_clone[b"HEAD"].id

        # Create a branch in the remote that doesn't exist in clone
        self.repo.refs[b"refs/heads/to-be-deleted"] = self.repo[b"HEAD"].id

        # Configure mirror mode
        with Repo(clone_path) as r_clone:
            config = r_clone.get_config()
            config.set((b"remote", b"origin"), b"mirror", True)
            config.write_to_path()

        # Push with mirror mode
        porcelain.push(
            clone_path,
            "origin",
            outstream=outstream,
            errstream=errstream,
        )

        # Verify refs were properly mirrored
        with Repo(clone_path) as r_clone:
            # All local branches should be pushed
            self.assertEqual(
                r_clone.refs[b"refs/heads/feature"],
                self.repo.refs[b"refs/heads/feature"],
            )
            # All tags should be pushed
            self.assertEqual(
                r_clone.refs[b"refs/tags/v1.0"], self.repo.refs[b"refs/tags/v1.0"]
            )
            # Remote tracking branches should be pushed
            self.assertEqual(
                r_clone.refs[b"refs/remotes/upstream/main"],
                self.repo.refs[b"refs/remotes/upstream/main"],
            )

        # Verify the extra branch was deleted
        self.assertNotIn(b"refs/heads/to-be-deleted", self.repo.refs)

    def test_mirror_mode_disabled(self) -> None:
        """Test that mirror mode is properly disabled when set to false."""
        outstream = BytesIO()
        errstream = BytesIO()

        # Create initial commit
        porcelain.commit(
            repo=self.repo.path,
            message=b"init",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.clone(
            self.repo.path, target=clone_path, errstream=errstream
        )
        target_repo.close()

        # Create a branch in the remote that doesn't exist in clone
        self.repo.refs[b"refs/heads/should-not-be-deleted"] = self.repo[b"HEAD"].id

        # Explicitly set mirror mode to false
        with Repo(clone_path) as r_clone:
            config = r_clone.get_config()
            config.set((b"remote", b"origin"), b"mirror", False)
            config.write_to_path()

        # Push normally (not mirror mode)
        porcelain.push(
            clone_path,
            "origin",
            outstream=outstream,
            errstream=errstream,
        )

        # Verify the extra branch was NOT deleted
        self.assertIn(b"refs/heads/should-not-be-deleted", self.repo.refs)


class PullTests(PorcelainTestCase):
    def setUp(self) -> None:
        super().setUp()
        # create a file for initial commit
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(
            repo=self.repo.path,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Setup target repo
        self.target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.target_path)
        target_repo = porcelain.clone(
            self.repo.path, target=self.target_path, errstream=BytesIO()
        )
        target_repo.close()

        # create a second file to be pushed
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(
            repo=self.repo.path,
            message=b"test2",
            author=b"test2 <email>",
            committer=b"test2 <email>",
        )

        self.assertIn(b"refs/heads/master", self.repo.refs)
        self.assertIn(b"refs/heads/master", target_repo.refs)

    def test_simple(self) -> None:
        outstream = BytesIO()
        errstream = BytesIO()

        # Pull changes into the cloned repo
        porcelain.pull(
            self.target_path,
            self.repo.path,
            b"refs/heads/master",
            outstream=outstream,
            errstream=errstream,
        )

        # Check the target repo for pushed changes
        with Repo(self.target_path) as r:
            self.assertEqual(r[b"HEAD"].id, self.repo[b"HEAD"].id)

    def test_diverged(self) -> None:
        outstream = BytesIO()
        errstream = BytesIO()

        c3a = porcelain.commit(
            repo=self.target_path,
            message=b"test3a",
            author=b"test2 <email>",
            committer=b"test2 <email>",
        )

        porcelain.commit(
            repo=self.repo.path,
            message=b"test3b",
            author=b"test2 <email>",
            committer=b"test2 <email>",
        )

        # Pull changes into the cloned repo
        self.assertRaises(
            porcelain.DivergedBranches,
            porcelain.pull,
            self.target_path,
            self.repo.path,
            b"refs/heads/master",
            outstream=outstream,
            errstream=errstream,
        )

        # Check the target repo for pushed changes
        with Repo(self.target_path) as r:
            self.assertEqual(r[b"refs/heads/master"].id, c3a)

        # Pull with merge should now work
        porcelain.pull(
            self.target_path,
            self.repo.path,
            b"refs/heads/master",
            outstream=outstream,
            errstream=errstream,
            fast_forward=False,
        )

        # Check the target repo for merged changes
        with Repo(self.target_path) as r:
            # HEAD should now be a merge commit
            head = r[b"HEAD"]
            # It should have two parents
            self.assertEqual(len(head.parents), 2)
            # One parent should be the previous HEAD (c3a)
            self.assertIn(c3a, head.parents)
            # The other parent should be from the source repo
            self.assertIn(self.repo[b"HEAD"].id, head.parents)

    def test_no_refspec(self) -> None:
        outstream = BytesIO()
        errstream = BytesIO()

        # Pull changes into the cloned repo
        porcelain.pull(
            self.target_path,
            self.repo.path,
            outstream=outstream,
            errstream=errstream,
        )

        # Check the target repo for pushed changes
        with Repo(self.target_path) as r:
            self.assertEqual(r[b"HEAD"].id, self.repo[b"HEAD"].id)

    def test_no_remote_location(self) -> None:
        outstream = BytesIO()
        errstream = BytesIO()

        # Pull changes into the cloned repo
        porcelain.pull(
            self.target_path,
            refspecs=b"refs/heads/master",
            outstream=outstream,
            errstream=errstream,
        )

        # Check the target repo for pushed changes
        with Repo(self.target_path) as r:
            self.assertEqual(r[b"HEAD"].id, self.repo[b"HEAD"].id)

    def test_pull_updates_working_tree(self) -> None:
        """Test that pull updates the working tree with new files."""
        outstream = BytesIO()
        errstream = BytesIO()

        # Create a new file with content in the source repo
        new_file = os.path.join(self.repo.path, "newfile.txt")
        with open(new_file, "w") as f:
            f.write("This is new content")

        porcelain.add(repo=self.repo.path, paths=[new_file])
        porcelain.commit(
            repo=self.repo.path,
            message=b"Add new file",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Before pull, the file should not exist in target
        target_file = os.path.join(self.target_path, "newfile.txt")
        self.assertFalse(os.path.exists(target_file))

        # Pull changes into the cloned repo
        porcelain.pull(
            self.target_path,
            self.repo.path,
            b"refs/heads/master",
            outstream=outstream,
            errstream=errstream,
        )

        # After pull, the file should exist with correct content
        self.assertTrue(os.path.exists(target_file))
        with open(target_file) as f:
            self.assertEqual(f.read(), "This is new content")

        # Check the HEAD is updated too
        with Repo(self.target_path) as r:
            self.assertEqual(r[b"HEAD"].id, self.repo[b"HEAD"].id)


class StatusTests(PorcelainTestCase):
    def test_empty(self) -> None:
        results = porcelain.status(self.repo)
        self.assertEqual({"add": [], "delete": [], "modify": []}, results.staged)
        self.assertEqual([], results.unstaged)

    def test_status_base(self) -> None:
        """Integration test for `status` functionality."""
        # Commit a dummy file then modify it
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("origstuff")

        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # modify access and modify time of path
        os.utime(fullpath, (0, 0))

        with open(fullpath, "wb") as f:
            f.write(b"stuff")

        # Make a dummy file and stage it
        filename_add = "bar"
        fullpath = os.path.join(self.repo.path, filename_add)
        with open(fullpath, "w") as f:
            f.write("stuff")
        porcelain.add(repo=self.repo.path, paths=fullpath)

        results = porcelain.status(self.repo)

        self.assertEqual(results.staged["add"][0], filename_add.encode("ascii"))
        self.assertEqual(results.unstaged, [b"foo"])

    def test_status_all(self) -> None:
        del_path = os.path.join(self.repo.path, "foo")
        mod_path = os.path.join(self.repo.path, "bar")
        add_path = os.path.join(self.repo.path, "baz")
        us_path = os.path.join(self.repo.path, "blye")
        ut_path = os.path.join(self.repo.path, "blyat")
        with open(del_path, "w") as f:
            f.write("origstuff")
        with open(mod_path, "w") as f:
            f.write("origstuff")
        with open(us_path, "w") as f:
            f.write("origstuff")
        porcelain.add(repo=self.repo.path, paths=[del_path, mod_path, us_path])
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )
        porcelain.remove(self.repo.path, [del_path])
        with open(add_path, "w") as f:
            f.write("origstuff")
        with open(mod_path, "w") as f:
            f.write("more_origstuff")
        with open(us_path, "w") as f:
            f.write("more_origstuff")
        porcelain.add(repo=self.repo.path, paths=[add_path, mod_path])
        with open(us_path, "w") as f:
            f.write("\norigstuff")
        with open(ut_path, "w") as f:
            f.write("origstuff")
        results = porcelain.status(self.repo.path)
        self.assertDictEqual(
            {"add": [b"baz"], "delete": [b"foo"], "modify": [b"bar"]},
            results.staged,
        )
        self.assertListEqual(results.unstaged, [b"blye"])
        results_no_untracked = porcelain.status(self.repo.path, untracked_files="no")
        self.assertListEqual(results_no_untracked.untracked, [])

    def test_status_wrong_untracked_files_value(self) -> None:
        with self.assertRaises(ValueError):
            porcelain.status(self.repo.path, untracked_files="antani")

    def test_status_untracked_path(self) -> None:
        untracked_dir = os.path.join(self.repo_path, "untracked_dir")
        os.mkdir(untracked_dir)
        untracked_file = os.path.join(untracked_dir, "untracked_file")
        with open(untracked_file, "w") as fh:
            fh.write("untracked")

        _, _, untracked = porcelain.status(self.repo.path, untracked_files="all")
        self.assertEqual(untracked, ["untracked_dir/untracked_file"])

    def test_status_untracked_path_normal(self) -> None:
        # Create an untracked directory with multiple files
        untracked_dir = os.path.join(self.repo_path, "untracked_dir")
        os.mkdir(untracked_dir)
        untracked_file1 = os.path.join(untracked_dir, "file1")
        untracked_file2 = os.path.join(untracked_dir, "file2")
        with open(untracked_file1, "w") as fh:
            fh.write("untracked1")
        with open(untracked_file2, "w") as fh:
            fh.write("untracked2")

        # Create a nested untracked directory
        nested_dir = os.path.join(untracked_dir, "nested")
        os.mkdir(nested_dir)
        nested_file = os.path.join(nested_dir, "file3")
        with open(nested_file, "w") as fh:
            fh.write("untracked3")

        # Test "normal" mode - should only show the directory, not individual files
        _, _, untracked = porcelain.status(self.repo.path, untracked_files="normal")
        self.assertEqual(untracked, ["untracked_dir/"])

        # Test "all" mode - should show all files
        _, _, untracked_all = porcelain.status(self.repo.path, untracked_files="all")
        self.assertEqual(
            sorted(untracked_all),
            [
                "untracked_dir/file1",
                "untracked_dir/file2",
                "untracked_dir/nested/file3",
            ],
        )

    def test_status_mixed_tracked_untracked(self) -> None:
        # Create a directory with both tracked and untracked files
        mixed_dir = os.path.join(self.repo_path, "mixed_dir")
        os.mkdir(mixed_dir)

        # Add a tracked file
        tracked_file = os.path.join(mixed_dir, "tracked.txt")
        with open(tracked_file, "w") as fh:
            fh.write("tracked content")
        porcelain.add(self.repo.path, paths=[tracked_file])
        porcelain.commit(
            repo=self.repo.path,
            message=b"add tracked file",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Add untracked files to the same directory
        untracked_file = os.path.join(mixed_dir, "untracked.txt")
        with open(untracked_file, "w") as fh:
            fh.write("untracked content")

        # In "normal" mode, should show individual untracked files in mixed dirs
        _, _, untracked = porcelain.status(self.repo.path, untracked_files="normal")
        self.assertEqual(untracked, ["mixed_dir/untracked.txt"])

        # In "all" mode, should be the same for mixed directories
        _, _, untracked_all = porcelain.status(self.repo.path, untracked_files="all")
        self.assertEqual(untracked_all, ["mixed_dir/untracked.txt"])

    def test_status_crlf_mismatch(self) -> None:
        # First make a commit as if the file has been added on a Linux system
        # or with core.autocrlf=True
        file_path = os.path.join(self.repo.path, "crlf")
        with open(file_path, "wb") as f:
            f.write(b"line1\nline2")
        porcelain.add(repo=self.repo.path, paths=[file_path])
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Then update the file as if it was created by CGit on a Windows
        # system with core.autocrlf=true
        with open(file_path, "wb") as f:
            f.write(b"line1\r\nline2")

        results = porcelain.status(self.repo)
        self.assertDictEqual({"add": [], "delete": [], "modify": []}, results.staged)
        self.assertListEqual(results.unstaged, [b"crlf"])
        self.assertListEqual(results.untracked, [])

    def test_status_autocrlf_true(self) -> None:
        # First make a commit as if the file has been added on a Linux system
        # or with core.autocrlf=True
        file_path = os.path.join(self.repo.path, "crlf")
        with open(file_path, "wb") as f:
            f.write(b"line1\nline2")
        porcelain.add(repo=self.repo.path, paths=[file_path])
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        # Then update the file as if it was created by CGit on a Windows
        # system with core.autocrlf=true
        with open(file_path, "wb") as f:
            f.write(b"line1\r\nline2")

        # TODO: It should be set automatically by looking at the configuration
        c = self.repo.get_config()
        c.set("core", "autocrlf", True)
        c.write_to_path()

        results = porcelain.status(self.repo)
        self.assertDictEqual({"add": [], "delete": [], "modify": []}, results.staged)
        self.assertListEqual(results.unstaged, [])
        self.assertListEqual(results.untracked, [])

    def test_status_autocrlf_input(self) -> None:
        # Commit existing file with CRLF
        file_path = os.path.join(self.repo.path, "crlf-exists")
        with open(file_path, "wb") as f:
            f.write(b"line1\r\nline2")
        porcelain.add(repo=self.repo.path, paths=[file_path])
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        c = self.repo.get_config()
        c.set("core", "autocrlf", "input")
        c.write_to_path()

        # Add new (untracked) file
        file_path = os.path.join(self.repo.path, "crlf-new")
        with open(file_path, "wb") as f:
            f.write(b"line1\r\nline2")
        porcelain.add(repo=self.repo.path, paths=[file_path])

        results = porcelain.status(self.repo)
        self.assertDictEqual(
            {"add": [b"crlf-new"], "delete": [], "modify": []}, results.staged
        )
        self.assertListEqual(results.unstaged, [])
        self.assertListEqual(results.untracked, [])

    def test_get_tree_changes_add(self) -> None:
        """Unit test for get_tree_changes add."""
        # Make a dummy file, stage
        filename = "bar"
        fullpath = os.path.join(self.repo.path, filename)
        with open(fullpath, "w") as f:
            f.write("stuff")
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        filename = "foo"
        fullpath = os.path.join(self.repo.path, filename)
        with open(fullpath, "w") as f:
            f.write("stuff")
        porcelain.add(repo=self.repo.path, paths=fullpath)
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes["add"][0], filename.encode("ascii"))
        self.assertEqual(len(changes["add"]), 1)
        self.assertEqual(len(changes["modify"]), 0)
        self.assertEqual(len(changes["delete"]), 0)

    def test_get_tree_changes_modify(self) -> None:
        """Unit test for get_tree_changes modify."""
        # Make a dummy file, stage, commit, modify
        filename = "foo"
        fullpath = os.path.join(self.repo.path, filename)
        with open(fullpath, "w") as f:
            f.write("stuff")
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )
        with open(fullpath, "w") as f:
            f.write("otherstuff")
        porcelain.add(repo=self.repo.path, paths=fullpath)
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes["modify"][0], filename.encode("ascii"))
        self.assertEqual(len(changes["add"]), 0)
        self.assertEqual(len(changes["modify"]), 1)
        self.assertEqual(len(changes["delete"]), 0)

    def test_get_tree_changes_delete(self) -> None:
        """Unit test for get_tree_changes delete."""
        # Make a dummy file, stage, commit, remove
        filename = "foo"
        fullpath = os.path.join(self.repo.path, filename)
        with open(fullpath, "w") as f:
            f.write("stuff")
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )
        cwd = os.getcwd()
        self.addCleanup(os.chdir, cwd)
        os.chdir(self.repo.path)
        porcelain.remove(repo=self.repo.path, paths=[filename])
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes["delete"][0], filename.encode("ascii"))
        self.assertEqual(len(changes["add"]), 0)
        self.assertEqual(len(changes["modify"]), 0)
        self.assertEqual(len(changes["delete"]), 1)

    def test_get_untracked_paths(self) -> None:
        with open(os.path.join(self.repo.path, ".gitignore"), "w") as f:
            f.write("ignored\n")
        with open(os.path.join(self.repo.path, "ignored"), "w") as f:
            f.write("blah\n")
        with open(os.path.join(self.repo.path, "notignored"), "w") as f:
            f.write("blah\n")
        os.symlink(
            os.path.join(self.repo.path, os.pardir, "external_target"),
            os.path.join(self.repo.path, "link"),
        )
        self.assertEqual(
            {"ignored", "notignored", ".gitignore", "link"},
            set(
                porcelain.get_untracked_paths(
                    self.repo.path, self.repo.path, self.repo.open_index()
                )
            ),
        )
        self.assertEqual(
            {".gitignore", "notignored", "link"},
            set(porcelain.status(self.repo).untracked),
        )
        self.assertEqual(
            {".gitignore", "notignored", "ignored", "link"},
            set(porcelain.status(self.repo, ignored=True).untracked),
        )

    def test_get_untracked_paths_subrepo(self) -> None:
        with open(os.path.join(self.repo.path, ".gitignore"), "w") as f:
            f.write("nested/\n")
        with open(os.path.join(self.repo.path, "notignored"), "w") as f:
            f.write("blah\n")

        subrepo = Repo.init(os.path.join(self.repo.path, "nested"), mkdir=True)
        with open(os.path.join(subrepo.path, "ignored"), "w") as f:
            f.write("bleep\n")
        with open(os.path.join(subrepo.path, "with"), "w") as f:
            f.write("bloop\n")
        with open(os.path.join(subrepo.path, "manager"), "w") as f:
            f.write("blop\n")

        self.assertEqual(
            {".gitignore", "notignored", os.path.join("nested", "")},
            set(
                porcelain.get_untracked_paths(
                    self.repo.path, self.repo.path, self.repo.open_index()
                )
            ),
        )
        self.assertEqual(
            {".gitignore", "notignored"},
            set(
                porcelain.get_untracked_paths(
                    self.repo.path,
                    self.repo.path,
                    self.repo.open_index(),
                    exclude_ignored=True,
                )
            ),
        )
        self.assertEqual(
            {"ignored", "with", "manager"},
            set(
                porcelain.get_untracked_paths(
                    subrepo.path, subrepo.path, subrepo.open_index()
                )
            ),
        )
        self.assertEqual(
            set(),
            set(
                porcelain.get_untracked_paths(
                    subrepo.path,
                    self.repo.path,
                    self.repo.open_index(),
                )
            ),
        )
        self.assertEqual(
            {
                os.path.join("nested", "ignored"),
                os.path.join("nested", "with"),
                os.path.join("nested", "manager"),
            },
            set(
                porcelain.get_untracked_paths(
                    self.repo.path,
                    subrepo.path,
                    self.repo.open_index(),
                )
            ),
        )

    def test_get_untracked_paths_subdir(self) -> None:
        with open(os.path.join(self.repo.path, ".gitignore"), "w") as f:
            f.write("subdir/\nignored")
        with open(os.path.join(self.repo.path, "notignored"), "w") as f:
            f.write("blah\n")
        os.mkdir(os.path.join(self.repo.path, "subdir"))
        with open(os.path.join(self.repo.path, "ignored"), "w") as f:
            f.write("foo")
        with open(os.path.join(self.repo.path, "subdir", "ignored"), "w") as f:
            f.write("foo")

        self.assertEqual(
            {
                ".gitignore",
                "notignored",
                "ignored",
                os.path.join("subdir", ""),
            },
            set(
                porcelain.get_untracked_paths(
                    self.repo.path,
                    self.repo.path,
                    self.repo.open_index(),
                )
            ),
        )
        self.assertEqual(
            {".gitignore", "notignored"},
            set(
                porcelain.get_untracked_paths(
                    self.repo.path,
                    self.repo.path,
                    self.repo.open_index(),
                    exclude_ignored=True,
                )
            ),
        )

    def test_get_untracked_paths_invalid_untracked_files(self) -> None:
        with self.assertRaises(ValueError):
            list(
                porcelain.get_untracked_paths(
                    self.repo.path,
                    self.repo.path,
                    self.repo.open_index(),
                    untracked_files="invalid_value",
                )
            )

    def test_get_untracked_paths_normal(self) -> None:
        # Create an untracked directory with files
        untracked_dir = os.path.join(self.repo.path, "untracked_dir")
        os.mkdir(untracked_dir)
        with open(os.path.join(untracked_dir, "file1.txt"), "w") as f:
            f.write("untracked content")
        with open(os.path.join(untracked_dir, "file2.txt"), "w") as f:
            f.write("more untracked content")

        # Test that "normal" mode works and returns only the directory
        _, _, untracked = porcelain.status(
            repo=self.repo.path, untracked_files="normal"
        )
        self.assertEqual(untracked, ["untracked_dir/"])

    def test_get_untracked_paths_top_level_issue_1247(self) -> None:
        """Test for issue #1247: ensure top-level untracked files are detected."""
        # Create a single top-level untracked file
        with open(os.path.join(self.repo.path, "sample.txt"), "w") as f:
            f.write("test content")

        # Test get_untracked_paths directly
        untracked = list(
            porcelain.get_untracked_paths(
                self.repo.path, self.repo.path, self.repo.open_index()
            )
        )
        self.assertIn(
            "sample.txt",
            untracked,
            "Top-level file 'sample.txt' should be in untracked list",
        )

        # Test via status
        status = porcelain.status(self.repo)
        self.assertIn(
            "sample.txt",
            status.untracked,
            "Top-level file 'sample.txt' should be in status.untracked",
        )


# TODO(jelmer): Add test for dulwich.porcelain.daemon


class UploadPackTests(PorcelainTestCase):
    """Tests for upload_pack."""

    def test_upload_pack(self) -> None:
        outf = BytesIO()
        exitcode = porcelain.upload_pack(self.repo.path, BytesIO(b"0000"), outf)
        outlines = outf.getvalue().splitlines()
        self.assertEqual([b"0000"], outlines)
        self.assertEqual(0, exitcode)


class ReceivePackTests(PorcelainTestCase):
    """Tests for receive_pack."""

    def test_receive_pack(self) -> None:
        filename = "foo"
        fullpath = os.path.join(self.repo.path, filename)
        with open(fullpath, "w") as f:
            f.write("stuff")
        porcelain.add(repo=self.repo.path, paths=fullpath)
        self.repo.do_commit(
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
            author_timestamp=1402354300,
            commit_timestamp=1402354300,
            author_timezone=0,
            commit_timezone=0,
        )
        outf = BytesIO()
        exitcode = porcelain.receive_pack(self.repo.path, BytesIO(b"0000"), outf)
        outlines = outf.getvalue().splitlines()
        self.assertEqual(
            [
                b"0091319b56ce3aee2d489f759736a79cc552c9bb86d9 HEAD\x00 report-status "
                b"delete-refs quiet ofs-delta side-band-64k "
                b"no-done symref=HEAD:refs/heads/master",
                b"003f319b56ce3aee2d489f759736a79cc552c9bb86d9 refs/heads/master",
                b"0000",
            ],
            outlines,
        )
        self.assertEqual(0, exitcode)


class BranchListTests(PorcelainTestCase):
    def test_standard(self) -> None:
        self.assertEqual(set(), set(porcelain.branch_list(self.repo)))

    def test_new_branch(self) -> None:
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.branch_create(self.repo, b"foo")
        self.assertEqual({b"master", b"foo"}, set(porcelain.branch_list(self.repo)))

    def test_sort_by_refname(self) -> None:
        """Test branch.sort=refname (default alphabetical)."""
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id

        # Create branches in non-alphabetical order
        porcelain.branch_create(self.repo, b"zebra")
        porcelain.branch_create(self.repo, b"alpha")
        porcelain.branch_create(self.repo, b"beta")

        # Set branch.sort to refname (though it's the default)
        config = self.repo.get_config()
        config.set((b"branch",), b"sort", b"refname")
        config.write_to_path()

        # Should be sorted alphabetically
        branches = porcelain.branch_list(self.repo)
        self.assertEqual([b"alpha", b"beta", b"master", b"zebra"], branches)

    def test_sort_by_refname_reverse(self) -> None:
        """Test branch.sort=-refname (reverse alphabetical)."""
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id

        # Create branches
        porcelain.branch_create(self.repo, b"zebra")
        porcelain.branch_create(self.repo, b"alpha")
        porcelain.branch_create(self.repo, b"beta")

        # Set branch.sort to -refname
        config = self.repo.get_config()
        config.set((b"branch",), b"sort", b"-refname")
        config.write_to_path()

        # Should be sorted reverse alphabetically
        branches = porcelain.branch_list(self.repo)
        self.assertEqual([b"zebra", b"master", b"beta", b"alpha"], branches)

    def test_sort_by_committerdate(self) -> None:
        """Test branch.sort=committerdate."""
        # Use build_commit_graph to create proper commits with specific times
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store,
            [[1], [2], [3]],
            attrs={
                1: {"commit_time": 1000},  # oldest
                2: {"commit_time": 2000},  # newest
                3: {"commit_time": 1500},  # middle
            },
        )

        self.repo[b"HEAD"] = c1.id

        # Create branches pointing to different commits
        self.repo.refs[b"refs/heads/master"] = c1.id  # master points to oldest
        self.repo.refs[b"refs/heads/oldest"] = c1.id
        self.repo.refs[b"refs/heads/newest"] = c2.id
        self.repo.refs[b"refs/heads/middle"] = c3.id

        # Set branch.sort to committerdate
        config = self.repo.get_config()
        config.set((b"branch",), b"sort", b"committerdate")
        config.write_to_path()

        # Should be sorted by commit time (oldest first)
        branches = porcelain.branch_list(self.repo)
        self.assertEqual([b"master", b"oldest", b"middle", b"newest"], branches)

    def test_sort_by_committerdate_reverse(self) -> None:
        """Test branch.sort=-committerdate."""
        # Use build_commit_graph to create proper commits with specific times
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store,
            [[1], [2], [3]],
            attrs={
                1: {"commit_time": 1000},  # oldest
                2: {"commit_time": 2000},  # newest
                3: {"commit_time": 1500},  # middle
            },
        )

        self.repo[b"HEAD"] = c1.id

        # Create branches pointing to different commits
        self.repo.refs[b"refs/heads/master"] = c1.id  # master points to oldest
        self.repo.refs[b"refs/heads/oldest"] = c1.id
        self.repo.refs[b"refs/heads/newest"] = c2.id
        self.repo.refs[b"refs/heads/middle"] = c3.id

        # Set branch.sort to -committerdate
        config = self.repo.get_config()
        config.set((b"branch",), b"sort", b"-committerdate")
        config.write_to_path()

        # Should be sorted by commit time (newest first)
        branches = porcelain.branch_list(self.repo)
        self.assertEqual([b"newest", b"middle", b"master", b"oldest"], branches)

    def test_sort_default(self) -> None:
        """Test default sorting (no config)."""
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id

        # Create branches in non-alphabetical order
        porcelain.branch_create(self.repo, b"zebra")
        porcelain.branch_create(self.repo, b"alpha")
        porcelain.branch_create(self.repo, b"beta")

        # No config set - should default to alphabetical
        branches = porcelain.branch_list(self.repo)
        self.assertEqual([b"alpha", b"beta", b"master", b"zebra"], branches)


class BranchCreateTests(PorcelainTestCase):
    def test_branch_exists(self) -> None:
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.branch_create(self.repo, b"foo")
        self.assertRaises(porcelain.Error, porcelain.branch_create, self.repo, b"foo")
        porcelain.branch_create(self.repo, b"foo", force=True)

    def test_new_branch(self) -> None:
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.branch_create(self.repo, b"foo")
        self.assertEqual({b"master", b"foo"}, set(porcelain.branch_list(self.repo)))

    def test_auto_setup_merge_true_from_remote_tracking(self) -> None:
        """Test branch.autoSetupMerge=true sets up tracking from remote-tracking branch."""
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        # Create a remote-tracking branch
        self.repo.refs[b"refs/remotes/origin/feature"] = c1.id

        # Set branch.autoSetupMerge to true (default)
        config = self.repo.get_config()
        config.set((b"branch",), b"autoSetupMerge", b"true")
        config.write_to_path()

        # Create branch from remote-tracking branch
        porcelain.branch_create(self.repo, "myfeature", "origin/feature")

        # Verify tracking was set up
        config = self.repo.get_config()
        self.assertEqual(config.get((b"branch", b"myfeature"), b"remote"), b"origin")
        self.assertEqual(
            config.get((b"branch", b"myfeature"), b"merge"), b"refs/heads/feature"
        )

    def test_auto_setup_merge_false(self) -> None:
        """Test branch.autoSetupMerge=false disables tracking setup."""
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        # Create a remote-tracking branch
        self.repo.refs[b"refs/remotes/origin/feature"] = c1.id

        # Set branch.autoSetupMerge to false
        config = self.repo.get_config()
        config.set((b"branch",), b"autoSetupMerge", b"false")
        config.write_to_path()

        # Create branch from remote-tracking branch
        porcelain.branch_create(self.repo, "myfeature", "origin/feature")

        # Verify tracking was NOT set up
        config = self.repo.get_config()
        self.assertRaises(KeyError, config.get, (b"branch", b"myfeature"), b"remote")
        self.assertRaises(KeyError, config.get, (b"branch", b"myfeature"), b"merge")

    def test_auto_setup_merge_always(self) -> None:
        """Test branch.autoSetupMerge=always sets up tracking even from local branches."""
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        self.repo.refs[b"refs/heads/main"] = c1.id

        # Set branch.autoSetupMerge to always
        config = self.repo.get_config()
        config.set((b"branch",), b"autoSetupMerge", b"always")
        config.write_to_path()

        # Create branch from local branch - normally wouldn't set up tracking
        porcelain.branch_create(self.repo, "feature", "main")

        # With always, tracking should NOT be set up from local branches
        # (Git only sets up tracking from remote-tracking branches even with always)
        config = self.repo.get_config()
        self.assertRaises(KeyError, config.get, (b"branch", b"feature"), b"remote")
        self.assertRaises(KeyError, config.get, (b"branch", b"feature"), b"merge")

    def test_auto_setup_merge_always_from_remote(self) -> None:
        """Test branch.autoSetupMerge=always still sets up tracking from remote branches."""
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        # Create a remote-tracking branch
        self.repo.refs[b"refs/remotes/origin/feature"] = c1.id

        # Set branch.autoSetupMerge to always
        config = self.repo.get_config()
        config.set((b"branch",), b"autoSetupMerge", b"always")
        config.write_to_path()

        # Create branch from remote-tracking branch
        porcelain.branch_create(self.repo, "myfeature", "origin/feature")

        # Verify tracking was set up
        config = self.repo.get_config()
        self.assertEqual(config.get((b"branch", b"myfeature"), b"remote"), b"origin")
        self.assertEqual(
            config.get((b"branch", b"myfeature"), b"merge"), b"refs/heads/feature"
        )

    def test_auto_setup_merge_default(self) -> None:
        """Test default behavior (no config) is same as true."""
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        # Create a remote-tracking branch
        self.repo.refs[b"refs/remotes/origin/feature"] = c1.id

        # Don't set any config - should default to true

        # Create branch from remote-tracking branch
        porcelain.branch_create(self.repo, "myfeature", "origin/feature")

        # Verify tracking was set up
        config = self.repo.get_config()
        self.assertEqual(config.get((b"branch", b"myfeature"), b"remote"), b"origin")
        self.assertEqual(
            config.get((b"branch", b"myfeature"), b"merge"), b"refs/heads/feature"
        )


class BranchDeleteTests(PorcelainTestCase):
    def test_simple(self) -> None:
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.branch_create(self.repo, b"foo")
        self.assertIn(b"foo", porcelain.branch_list(self.repo))
        porcelain.branch_delete(self.repo, b"foo")
        self.assertNotIn(b"foo", porcelain.branch_list(self.repo))

    def test_simple_unicode(self) -> None:
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.branch_create(self.repo, "foo")
        self.assertIn(b"foo", porcelain.branch_list(self.repo))
        porcelain.branch_delete(self.repo, "foo")
        self.assertNotIn(b"foo", porcelain.branch_list(self.repo))


class FetchTests(PorcelainTestCase):
    def test_simple(self) -> None:
        outstream = BytesIO()
        errstream = BytesIO()

        # create a file for initial commit
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(
            repo=self.repo.path,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Setup target repo
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        target_repo = porcelain.clone(
            self.repo.path, target=target_path, errstream=errstream
        )

        # create a second file to be pushed
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(
            repo=self.repo.path,
            message=b"test2",
            author=b"test2 <email>",
            committer=b"test2 <email>",
        )

        self.assertNotIn(self.repo[b"HEAD"].id, target_repo)
        target_repo.close()

        # Fetch changes into the cloned repo
        porcelain.fetch(target_path, "origin", outstream=outstream, errstream=errstream)

        # Assert that fetch updated the local image of the remote
        self.assert_correct_remote_refs(target_repo.get_refs(), self.repo.get_refs())

        # Check the target repo for pushed changes
        with Repo(target_path) as r:
            self.assertIn(self.repo[b"HEAD"].id, r)

    def test_with_remote_name(self) -> None:
        remote_name = "origin"
        outstream = BytesIO()
        errstream = BytesIO()

        # create a file for initial commit
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(
            repo=self.repo.path,
            message=b"test",
            author=b"test <email>",
            committer=b"test <email>",
        )

        # Setup target repo
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        target_repo = porcelain.clone(
            self.repo.path, target=target_path, errstream=errstream
        )

        # Capture current refs
        target_refs = target_repo.get_refs()

        # create a second file to be pushed
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(
            repo=self.repo.path,
            message=b"test2",
            author=b"test2 <email>",
            committer=b"test2 <email>",
        )

        self.assertNotIn(self.repo[b"HEAD"].id, target_repo)

        target_config = target_repo.get_config()
        target_config.set(
            (b"remote", remote_name.encode()), b"url", self.repo.path.encode()
        )
        target_repo.close()

        # Fetch changes into the cloned repo
        porcelain.fetch(
            target_path, remote_name, outstream=outstream, errstream=errstream
        )

        # Assert that fetch updated the local image of the remote
        self.assert_correct_remote_refs(target_repo.get_refs(), self.repo.get_refs())

        # Check the target repo for pushed changes, as well as updates
        # for the refs
        with Repo(target_path) as r:
            self.assertIn(self.repo[b"HEAD"].id, r)
            self.assertNotEqual(self.repo.get_refs(), target_refs)

    def assert_correct_remote_refs(
        self, local_refs, remote_refs, remote_name=b"origin"
    ) -> None:
        """Assert that known remote refs corresponds to actual remote refs."""
        local_ref_prefix = b"refs/heads"
        remote_ref_prefix = b"refs/remotes/" + remote_name

        locally_known_remote_refs = {
            k[len(remote_ref_prefix) + 1 :]: v
            for k, v in local_refs.items()
            if k.startswith(remote_ref_prefix)
        }

        normalized_remote_refs = {
            k[len(local_ref_prefix) + 1 :]: v
            for k, v in remote_refs.items()
            if k.startswith(local_ref_prefix)
        }
        if b"HEAD" in locally_known_remote_refs and b"HEAD" in remote_refs:
            normalized_remote_refs[b"HEAD"] = remote_refs[b"HEAD"]

        self.assertEqual(locally_known_remote_refs, normalized_remote_refs)


class RepackTests(PorcelainTestCase):
    def test_empty(self) -> None:
        porcelain.repack(self.repo)

    def test_simple(self) -> None:
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.repack(self.repo)


class LsTreeTests(PorcelainTestCase):
    def test_empty(self) -> None:
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        f = StringIO()
        porcelain.ls_tree(self.repo, b"HEAD", outstream=f)
        self.assertEqual(f.getvalue(), "")

    def test_simple(self) -> None:
        # Commit a dummy file then modify it
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("origstuff")

        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        output = StringIO()
        porcelain.ls_tree(self.repo, b"HEAD", outstream=output)
        self.assertEqual(
            output.getvalue(),
            "100644 blob 8b82634d7eae019850bb883f06abf428c58bc9aa\tfoo\n",
        )

    def test_recursive(self) -> None:
        # Create a directory then write a dummy file in it
        dirpath = os.path.join(self.repo.path, "adir")
        filepath = os.path.join(dirpath, "afile")
        os.mkdir(dirpath)
        with open(filepath, "w") as f:
            f.write("origstuff")
        porcelain.add(repo=self.repo.path, paths=[filepath])
        porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )
        output = StringIO()
        porcelain.ls_tree(self.repo, b"HEAD", outstream=output)
        self.assertEqual(
            output.getvalue(),
            "40000 tree b145cc69a5e17693e24d8a7be0016ed8075de66d\tadir\n",
        )
        output2 = StringIO()
        porcelain.ls_tree(self.repo, b"HEAD", outstream=output2, recursive=True)
        self.assertEqual(
            output2.getvalue(),
            "40000 tree b145cc69a5e17693e24d8a7be0016ed8075de66d\tadir\n"
            "100644 blob 8b82634d7eae019850bb883f06abf428c58bc9aa\tadir"
            "/afile\n",
        )


class LsRemoteTests(PorcelainTestCase):
    def test_empty(self) -> None:
        result = porcelain.ls_remote(self.repo.path)
        self.assertEqual({}, result.refs)
        self.assertEqual({}, result.symrefs)

    def test_some(self) -> None:
        cid = porcelain.commit(
            repo=self.repo.path,
            message=b"test status",
            author=b"author <email>",
            committer=b"committer <email>",
        )

        result = porcelain.ls_remote(self.repo.path)
        self.assertEqual(
            {b"refs/heads/master": cid, b"HEAD": cid},
            result.refs,
        )
        # HEAD should be a symref to refs/heads/master
        self.assertEqual({b"HEAD": b"refs/heads/master"}, result.symrefs)


class LsFilesTests(PorcelainTestCase):
    def test_empty(self) -> None:
        self.assertEqual([], list(porcelain.ls_files(self.repo)))

    def test_simple(self) -> None:
        # Commit a dummy file then modify it
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("origstuff")

        porcelain.add(repo=self.repo.path, paths=[fullpath])
        self.assertEqual([b"foo"], list(porcelain.ls_files(self.repo)))


class RemoteAddTests(PorcelainTestCase):
    def test_new(self) -> None:
        porcelain.remote_add(self.repo, "jelmer", "git://jelmer.uk/code/dulwich")
        c = self.repo.get_config()
        self.assertEqual(
            c.get((b"remote", b"jelmer"), b"url"),
            b"git://jelmer.uk/code/dulwich",
        )

    def test_exists(self) -> None:
        porcelain.remote_add(self.repo, "jelmer", "git://jelmer.uk/code/dulwich")
        self.assertRaises(
            porcelain.RemoteExists,
            porcelain.remote_add,
            self.repo,
            "jelmer",
            "git://jelmer.uk/code/dulwich",
        )


class RemoteRemoveTests(PorcelainTestCase):
    def test_remove(self) -> None:
        porcelain.remote_add(self.repo, "jelmer", "git://jelmer.uk/code/dulwich")
        c = self.repo.get_config()
        self.assertEqual(
            c.get((b"remote", b"jelmer"), b"url"),
            b"git://jelmer.uk/code/dulwich",
        )
        porcelain.remote_remove(self.repo, "jelmer")
        self.assertRaises(KeyError, porcelain.remote_remove, self.repo, "jelmer")
        c = self.repo.get_config()
        self.assertRaises(KeyError, c.get, (b"remote", b"jelmer"), b"url")


class CheckIgnoreTests(PorcelainTestCase):
    def test_check_ignored(self) -> None:
        with open(os.path.join(self.repo.path, ".gitignore"), "w") as f:
            f.write("foo")
        foo_path = os.path.join(self.repo.path, "foo")
        with open(foo_path, "w") as f:
            f.write("BAR")
        bar_path = os.path.join(self.repo.path, "bar")
        with open(bar_path, "w") as f:
            f.write("BAR")
        self.assertEqual(["foo"], list(porcelain.check_ignore(self.repo, [foo_path])))
        self.assertEqual([], list(porcelain.check_ignore(self.repo, [bar_path])))

    def test_check_added_abs(self) -> None:
        path = os.path.join(self.repo.path, "foo")
        with open(path, "w") as f:
            f.write("BAR")
        self.repo.stage(["foo"])
        with open(os.path.join(self.repo.path, ".gitignore"), "w") as f:
            f.write("foo\n")
        self.assertEqual([], list(porcelain.check_ignore(self.repo, [path])))
        self.assertEqual(
            ["foo"],
            list(porcelain.check_ignore(self.repo, [path], no_index=True)),
        )

    def test_check_added_rel(self) -> None:
        with open(os.path.join(self.repo.path, "foo"), "w") as f:
            f.write("BAR")
        self.repo.stage(["foo"])
        with open(os.path.join(self.repo.path, ".gitignore"), "w") as f:
            f.write("foo\n")
        cwd = os.getcwd()
        self.addCleanup(os.chdir, cwd)
        os.mkdir(os.path.join(self.repo.path, "bar"))
        os.chdir(os.path.join(self.repo.path, "bar"))
        self.assertEqual(list(porcelain.check_ignore(self.repo, ["../foo"])), [])
        self.assertEqual(
            ["../foo"],
            list(porcelain.check_ignore(self.repo, ["../foo"], no_index=True)),
        )


class UpdateHeadTests(PorcelainTestCase):
    def test_set_to_branch(self) -> None:
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo.refs[b"refs/heads/blah"] = c1.id
        porcelain.update_head(self.repo, "blah")
        self.assertEqual(c1.id, self.repo.head())
        self.assertEqual(b"ref: refs/heads/blah", self.repo.refs.read_ref(b"HEAD"))

    def test_set_to_branch_detached(self) -> None:
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo.refs[b"refs/heads/blah"] = c1.id
        porcelain.update_head(self.repo, "blah", detached=True)
        self.assertEqual(c1.id, self.repo.head())
        self.assertEqual(c1.id, self.repo.refs.read_ref(b"HEAD"))

    def test_set_to_commit_detached(self) -> None:
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo.refs[b"refs/heads/blah"] = c1.id
        porcelain.update_head(self.repo, c1.id, detached=True)
        self.assertEqual(c1.id, self.repo.head())
        self.assertEqual(c1.id, self.repo.refs.read_ref(b"HEAD"))

    def test_set_new_branch(self) -> None:
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo.refs[b"refs/heads/blah"] = c1.id
        porcelain.update_head(self.repo, "blah", new_branch="bar")
        self.assertEqual(c1.id, self.repo.head())
        self.assertEqual(b"ref: refs/heads/bar", self.repo.refs.read_ref(b"HEAD"))


class MailmapTests(PorcelainTestCase):
    def test_no_mailmap(self) -> None:
        self.assertEqual(
            b"Jelmer Vernooij <jelmer@samba.org>",
            porcelain.check_mailmap(self.repo, b"Jelmer Vernooij <jelmer@samba.org>"),
        )

    def test_mailmap_lookup(self) -> None:
        with open(os.path.join(self.repo.path, ".mailmap"), "wb") as f:
            f.write(
                b"""\
Jelmer Vernooij <jelmer@debian.org>
"""
            )
        self.assertEqual(
            b"Jelmer Vernooij <jelmer@debian.org>",
            porcelain.check_mailmap(self.repo, b"Jelmer Vernooij <jelmer@samba.org>"),
        )


class FsckTests(PorcelainTestCase):
    def test_none(self) -> None:
        self.assertEqual([], list(porcelain.fsck(self.repo)))

    def test_git_dir(self) -> None:
        obj = Tree()
        a = Blob()
        a.data = b"foo"
        obj.add(b".git", 0o100644, a.id)
        self.repo.object_store.add_objects([(a, None), (obj, None)])
        self.assertEqual(
            [(obj.id, "invalid name .git")],
            [(sha, str(e)) for (sha, e) in porcelain.fsck(self.repo)],
        )


class DescribeTests(PorcelainTestCase):
    def test_no_commits(self) -> None:
        self.assertRaises(KeyError, porcelain.describe, self.repo.path)

    def test_single_commit(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        sha = porcelain.commit(
            self.repo.path,
            message=b"Some message",
            author=b"Joe <joe@example.com>",
            committer=b"Bob <bob@example.com>",
        )
        self.assertEqual(
            "g{}".format(sha[:7].decode("ascii")),
            porcelain.describe(self.repo.path),
        )

    def test_tag(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Some message",
            author=b"Joe <joe@example.com>",
            committer=b"Bob <bob@example.com>",
        )
        porcelain.tag_create(
            self.repo.path,
            b"tryme",
            b"foo <foo@bar.com>",
            b"bar",
            annotated=True,
        )
        self.assertEqual("tryme", porcelain.describe(self.repo.path))

    def test_tag_and_commit(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Some message",
            author=b"Joe <joe@example.com>",
            committer=b"Bob <bob@example.com>",
        )
        porcelain.tag_create(
            self.repo.path,
            b"tryme",
            b"foo <foo@bar.com>",
            b"bar",
            annotated=True,
        )
        with open(fullpath, "w") as f:
            f.write("BAR2")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        sha = porcelain.commit(
            self.repo.path,
            message=b"Some message",
            author=b"Joe <joe@example.com>",
            committer=b"Bob <bob@example.com>",
        )
        self.assertEqual(
            "tryme-1-g{}".format(sha[:7].decode("ascii")),
            porcelain.describe(self.repo.path),
        )

    def test_tag_and_commit_full(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Some message",
            author=b"Joe <joe@example.com>",
            committer=b"Bob <bob@example.com>",
        )
        porcelain.tag_create(
            self.repo.path,
            b"tryme",
            b"foo <foo@bar.com>",
            b"bar",
            annotated=True,
        )
        with open(fullpath, "w") as f:
            f.write("BAR2")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        sha = porcelain.commit(
            self.repo.path,
            message=b"Some message",
            author=b"Joe <joe@example.com>",
            committer=b"Bob <bob@example.com>",
        )
        self.assertEqual(
            "tryme-1-g{}".format(sha.decode("ascii")),
            porcelain.describe(self.repo.path, abbrev=40),
        )

    def test_untagged_commit_abbreviation(self) -> None:
        _, _, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id
        brief_description, complete_description = (
            porcelain.describe(self.repo),
            porcelain.describe(self.repo, abbrev=40),
        )
        self.assertTrue(complete_description.startswith(brief_description))
        self.assertEqual(
            "g{}".format(c3.id.decode("ascii")),
            complete_description,
        )

    def test_hash_length_dynamic(self) -> None:
        """Test that hash length adjusts based on uniqueness."""
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("content")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        sha = porcelain.commit(
            self.repo.path,
            message=b"commit",
            author=b"Joe <joe@example.com>",
            committer=b"Bob <bob@example.com>",
        )

        # When abbrev is None, it should use find_unique_abbrev
        result = porcelain.describe(self.repo.path)
        # Should start with 'g' and have at least 7 characters
        self.assertTrue(result.startswith("g"))
        self.assertGreaterEqual(len(result[1:]), 7)
        # Should be a prefix of the full SHA
        self.assertTrue(sha.decode("ascii").startswith(result[1:]))


class PathToTreeTests(PorcelainTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.fp = os.path.join(self.test_dir, "bar")
        with open(self.fp, "w") as f:
            f.write("something")
        oldcwd = os.getcwd()
        self.addCleanup(os.chdir, oldcwd)
        os.chdir(self.test_dir)

    def test_path_to_tree_path_base(self) -> None:
        self.assertEqual(b"bar", porcelain.path_to_tree_path(self.test_dir, self.fp))
        self.assertEqual(b"bar", porcelain.path_to_tree_path(".", "./bar"))
        self.assertEqual(b"bar", porcelain.path_to_tree_path(".", "bar"))
        cwd = os.getcwd()
        self.assertEqual(
            b"bar", porcelain.path_to_tree_path(".", os.path.join(cwd, "bar"))
        )
        self.assertEqual(b"bar", porcelain.path_to_tree_path(cwd, "bar"))

    def test_path_to_tree_path_syntax(self) -> None:
        self.assertEqual(b"bar", porcelain.path_to_tree_path(".", "./bar"))

    def test_path_to_tree_path_error(self) -> None:
        with self.assertRaises(ValueError):
            with tempfile.TemporaryDirectory() as od:
                porcelain.path_to_tree_path(od, self.fp)

    def test_path_to_tree_path_rel(self) -> None:
        cwd = os.getcwd()
        self.addCleanup(os.chdir, cwd)
        os.mkdir(os.path.join(self.repo.path, "foo"))
        os.mkdir(os.path.join(self.repo.path, "foo/bar"))
        os.chdir(os.path.join(self.repo.path, "foo/bar"))
        with open("baz", "w") as f:
            f.write("contents")
        self.assertEqual(b"bar/baz", porcelain.path_to_tree_path("..", "baz"))
        self.assertEqual(
            b"bar/baz",
            porcelain.path_to_tree_path(
                os.path.join(os.getcwd(), ".."),
                os.path.join(os.getcwd(), "baz"),
            ),
        )
        self.assertEqual(
            b"bar/baz",
            porcelain.path_to_tree_path("..", os.path.join(os.getcwd(), "baz")),
        )
        self.assertEqual(
            b"bar/baz",
            porcelain.path_to_tree_path(os.path.join(os.getcwd(), ".."), "baz"),
        )


class GetObjectByPathTests(PorcelainTestCase):
    def test_simple(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Some message",
            author=b"Joe <joe@example.com>",
            committer=b"Bob <bob@example.com>",
        )
        self.assertEqual(b"BAR", porcelain.get_object_by_path(self.repo, "foo").data)
        self.assertEqual(b"BAR", porcelain.get_object_by_path(self.repo, b"foo").data)

    def test_encoding(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
            self.repo.path,
            message=b"Some message",
            author=b"Joe <joe@example.com>",
            committer=b"Bob <bob@example.com>",
            encoding=b"utf-8",
        )
        self.assertEqual(b"BAR", porcelain.get_object_by_path(self.repo, "foo").data)
        self.assertEqual(b"BAR", porcelain.get_object_by_path(self.repo, b"foo").data)

    def test_missing(self) -> None:
        self.assertRaises(KeyError, porcelain.get_object_by_path, self.repo, "foo")


class WriteTreeTests(PorcelainTestCase):
    def test_simple(self) -> None:
        fullpath = os.path.join(self.repo.path, "foo")
        with open(fullpath, "w") as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        self.assertEqual(
            b"d2092c8a9f311f0311083bf8d177f2ca0ab5b241",
            porcelain.write_tree(self.repo),
        )


class ActiveBranchTests(PorcelainTestCase):
    def test_simple(self) -> None:
        self.assertEqual(b"master", porcelain.active_branch(self.repo))


class BranchTrackingTests(PorcelainTestCase):
    def test_get_branch_merge(self) -> None:
        # Set up branch tracking configuration
        config = self.repo.get_config()
        config.set((b"branch", b"master"), b"remote", b"origin")
        config.set((b"branch", b"master"), b"merge", b"refs/heads/main")
        config.write_to_path()

        # Test getting merge ref for current branch
        merge_ref = porcelain.get_branch_merge(self.repo)
        self.assertEqual(b"refs/heads/main", merge_ref)

        # Test getting merge ref for specific branch
        merge_ref = porcelain.get_branch_merge(self.repo, b"master")
        self.assertEqual(b"refs/heads/main", merge_ref)

        # Test branch without merge config
        with self.assertRaises(KeyError):
            porcelain.get_branch_merge(self.repo, b"nonexistent")

    def test_set_branch_tracking(self) -> None:
        # Create a new branch
        sha, _ = _commit_file_with_content(self.repo, "foo", "content\n")
        porcelain.branch_create(self.repo, "feature")

        # Set up tracking
        porcelain.set_branch_tracking(
            self.repo, b"feature", b"upstream", b"refs/heads/main"
        )

        # Verify configuration was written
        config = self.repo.get_config()
        self.assertEqual(b"upstream", config.get((b"branch", b"feature"), b"remote"))
        self.assertEqual(
            b"refs/heads/main", config.get((b"branch", b"feature"), b"merge")
        )


class FindUniqueAbbrevTests(PorcelainTestCase):
    def test_simple(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        self.assertEqual(
            c1.id.decode("ascii")[:7],
            porcelain.find_unique_abbrev(self.repo.object_store, c1.id),
        )


class PackRefsTests(PorcelainTestCase):
    def test_all(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        self.repo.refs[b"refs/heads/master"] = c2.id
        self.repo.refs[b"refs/tags/foo"] = c1.id

        porcelain.pack_refs(self.repo, all=True)

        self.assertEqual(
            self.repo.refs.get_packed_refs(),
            {
                b"refs/heads/master": c2.id,
                b"refs/tags/foo": c1.id,
            },
        )

    def test_not_all(self) -> None:
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id
        self.repo.refs[b"refs/heads/master"] = c2.id
        self.repo.refs[b"refs/tags/foo"] = c1.id

        porcelain.pack_refs(self.repo)

        self.assertEqual(
            self.repo.refs.get_packed_refs(),
            {
                b"refs/tags/foo": c1.id,
            },
        )


class ServerTests(PorcelainTestCase):
    @contextlib.contextmanager
    def _serving(self):
        with make_server("localhost", 0, self.app) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()

            try:
                yield f"http://localhost:{server.server_port}"

            finally:
                server.shutdown()
                thread.join(10)

    def setUp(self) -> None:
        super().setUp()

        self.served_repo_path = os.path.join(self.test_dir, "served_repo.git")
        self.served_repo = Repo.init_bare(self.served_repo_path, mkdir=True)
        self.addCleanup(self.served_repo.close)

        backend = DictBackend({"/": self.served_repo})
        self.app = make_wsgi_chain(backend)

    def test_pull(self) -> None:
        (c1,) = build_commit_graph(self.served_repo.object_store, [[1]])
        self.served_repo.refs[b"refs/heads/master"] = c1.id

        with self._serving() as url:
            porcelain.pull(self.repo, url, "master")

    def test_push(self) -> None:
        (c1,) = build_commit_graph(self.repo.object_store, [[1]])
        self.repo.refs[b"refs/heads/master"] = c1.id

        with self._serving() as url:
            porcelain.push(self.repo, url, "master")


class ForEachTests(PorcelainTestCase):
    def setUp(self) -> None:
        super().setUp()
        c1, c2, c3, c4 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2], [4]]
        )
        porcelain.tag_create(
            self.repo.path,
            b"v0.1",
            objectish=c1.id,
            annotated=True,
            message=b"0.1",
        )
        porcelain.tag_create(
            self.repo.path,
            b"v1.0",
            objectish=c2.id,
            annotated=True,
            message=b"1.0",
        )
        porcelain.tag_create(self.repo.path, b"simple-tag", objectish=c3.id)
        porcelain.tag_create(
            self.repo.path,
            b"v1.1",
            objectish=c4.id,
            annotated=True,
            message=b"1.1",
        )
        porcelain.branch_create(
            self.repo.path, b"feat", objectish=c2.id.decode("ascii")
        )
        self.repo.refs[b"HEAD"] = c4.id

    def test_for_each_ref(self) -> None:
        refs = porcelain.for_each_ref(self.repo)

        self.assertEqual(
            [(object_type, tag) for _, object_type, tag in refs],
            [
                (b"commit", b"refs/heads/feat"),
                (b"commit", b"refs/heads/master"),
                (b"commit", b"refs/tags/simple-tag"),
                (b"tag", b"refs/tags/v0.1"),
                (b"tag", b"refs/tags/v1.0"),
                (b"tag", b"refs/tags/v1.1"),
            ],
        )

    def test_for_each_ref_pattern(self) -> None:
        versions = porcelain.for_each_ref(self.repo, pattern="refs/tags/v*")
        self.assertEqual(
            [(object_type, tag) for _, object_type, tag in versions],
            [
                (b"tag", b"refs/tags/v0.1"),
                (b"tag", b"refs/tags/v1.0"),
                (b"tag", b"refs/tags/v1.1"),
            ],
        )

        versions = porcelain.for_each_ref(self.repo, pattern="refs/tags/v1.?")
        self.assertEqual(
            [(object_type, tag) for _, object_type, tag in versions],
            [
                (b"tag", b"refs/tags/v1.0"),
                (b"tag", b"refs/tags/v1.1"),
            ],
        )


class SparseCheckoutTests(PorcelainTestCase):
    """Integration tests for Dulwich's sparse checkout feature."""

    # NOTE: We do NOT override `setUp()` here because the parent class
    #       (PorcelainTestCase) already:
    #         1) Creates self.test_dir = a unique temp dir
    #         2) Creates a subdir named "repo"
    #         3) Calls Repo.init() on that path
    #       Re-initializing again caused FileExistsError.

    #
    # Utility/Placeholder
    #
    def sparse_checkout(self, repo, patterns, force=False):
        """Wrapper around the actual porcelain.sparse_checkout function
        to handle any test-specific setup or logging.
        """
        return porcelain.sparse_checkout(repo, patterns, force=force)

    def _write_file(self, rel_path, content):
        """Helper to write a file in the repository working tree."""
        abs_path = os.path.join(self.repo_path, rel_path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "w") as f:
            f.write(content)
        return abs_path

    def _commit_file(self, rel_path, content):
        """Helper to write, add, and commit a file."""
        abs_path = self._write_file(rel_path, content)
        add(self.repo_path, paths=[abs_path])
        commit(self.repo_path, message=b"Added " + rel_path.encode("utf-8"))

    def _list_wtree_files(self):
        """Return a set of all files (not dirs) present
        in the working tree, ignoring .git/.
        """
        found_files = set()
        for root, dirs, files in os.walk(self.repo_path):
            # Skip .git in the walk
            if ".git" in dirs:
                dirs.remove(".git")

            for filename in files:
                file_rel = os.path.relpath(os.path.join(root, filename), self.repo_path)
                found_files.add(file_rel)
        return found_files

    def test_only_included_paths_appear_in_wtree(self):
        """Only included paths remain in the working tree, excluded paths are removed.

        Commits two files, "keep_me.txt" and "exclude_me.txt". Then applies a
        sparse-checkout pattern containing only "keep_me.txt". Ensures that
        the latter remains in the working tree, while "exclude_me.txt" is
        removed. This verifies correct application of sparse-checkout patterns
        to remove files not listed.
        """
        self._commit_file("keep_me.txt", "I'll stay\n")
        self._commit_file("exclude_me.txt", "I'll be excluded\n")

        patterns = ["keep_me.txt"]
        self.sparse_checkout(self.repo, patterns)

        actual_files = self._list_wtree_files()
        expected_files = {"keep_me.txt"}
        self.assertEqual(
            expected_files,
            actual_files,
            f"Expected only {expected_files}, but found {actual_files}",
        )

    def test_previously_included_paths_become_excluded(self):
        """Previously included files become excluded after pattern changes.

        Verifies that files initially brought into the working tree (e.g.,
        by including `data/`) can later be excluded by narrowing the
        sparse-checkout pattern to just `data/included_1.txt`. Confirms that
        the file `data/included_2.txt` remains in the index with
        skip-worktree set (rather than being removed entirely), ensuring
        data is not lost and Dulwich correctly updates the index flags.
        """
        self._commit_file("data/included_1.txt", "some content\n")
        self._commit_file("data/included_2.txt", "other content\n")

        initial_patterns = ["data/"]
        self.sparse_checkout(self.repo, initial_patterns)

        updated_patterns = ["data/included_1.txt"]
        self.sparse_checkout(self.repo, updated_patterns)

        actual_files = self._list_wtree_files()
        expected_files = {os.path.join("data", "included_1.txt")}
        self.assertEqual(expected_files, actual_files)

        idx = self.repo.open_index()
        self.assertIn(b"data/included_2.txt", idx)
        entry = idx[b"data/included_2.txt"]
        self.assertTrue(entry.skip_worktree)

    def test_force_removes_local_changes_for_excluded_paths(self):
        """Forced sparse checkout removes local modifications for newly excluded paths.

        Verifies that specifying force=True allows destructive operations
        which discard uncommitted changes. First, we commit "file1.txt" and
        then modify it. Next, we apply a pattern that excludes the file,
        using force=True. The local modifications (and the file) should
        be removed, leaving the working tree empty.
        """
        self._commit_file("file1.txt", "original content\n")

        file1_path = os.path.join(self.repo_path, "file1.txt")
        with open(file1_path, "a") as f:
            f.write("local changes!\n")

        new_patterns = ["some_other_file.txt"]
        self.sparse_checkout(self.repo, new_patterns, force=True)

        actual_files = self._list_wtree_files()
        self.assertEqual(
            set(),
            actual_files,
            "Force-sparse-checkout did not remove file with local changes.",
        )

    def test_destructive_refuse_uncommitted_changes_without_force(self):
        """Fail on uncommitted changes for newly excluded paths without force.

        Ensures that a sparse checkout is blocked if it would remove local
        modifications from the working tree. We commit 'config.yaml', then
        modify it, and finally attempt to exclude it via new patterns without
        using force=True. This should raise a CheckoutError rather than
        discarding the local changes.
        """
        self._commit_file("config.yaml", "initial\n")
        cfg_path = os.path.join(self.repo_path, "config.yaml")
        with open(cfg_path, "a") as f:
            f.write("local modifications\n")

        exclude_patterns = ["docs/"]
        with self.assertRaises(CheckoutError):
            self.sparse_checkout(self.repo, exclude_patterns, force=False)

    def test_fnmatch_gitignore_pattern_expansion(self):
        """Reading/writing patterns align with gitignore/fnmatch expansions.

        Ensures that `sparse_checkout` interprets wildcard patterns (like `*.py`)
        in the same way Git's sparse-checkout would. Multiple files are committed
        to `src/` (e.g. `foo.py`, `foo_test.py`, `foo_helper.py`) and to `docs/`.
        Then the pattern `src/foo*.py` is applied, confirming that only the
        matching Python files remain in the working tree while the Markdown file
        under `docs/` is excluded.

        Finally, verifies that the `.git/info/sparse-checkout` file contains the
        specified wildcard pattern (`src/foo*.py`), ensuring correct round-trip
        of user-supplied patterns.
        """
        self._commit_file("src/foo.py", "print('hello')\n")
        self._commit_file("src/foo_test.py", "print('test')\n")
        self._commit_file("docs/readme.md", "# docs\n")
        self._commit_file("src/foo_helper.py", "print('helper')\n")

        patterns = ["src/foo*.py"]
        self.sparse_checkout(self.repo, patterns)

        actual_files = self._list_wtree_files()
        expected_files = {
            os.path.join("src", "foo.py"),
            os.path.join("src", "foo_test.py"),
            os.path.join("src", "foo_helper.py"),
        }
        self.assertEqual(
            expected_files,
            actual_files,
            "Wildcard pattern not matched as expected. Either too strict or too broad.",
        )

        sc_file = os.path.join(self.repo_path, ".git", "info", "sparse-checkout")
        self.assertTrue(os.path.isfile(sc_file))
        with open(sc_file) as f:
            lines = f.read().strip().split()
            self.assertIn("src/foo*.py", lines)


class ConeModeTests(PorcelainTestCase):
    """Provide integration tests for Dulwich's cone mode sparse checkout.

    This test suite verifies the expected behavior for:
      * cone_mode_init
      * cone_mode_set
      * cone_mode_add
    Although Dulwich does not yet implement cone mode, these tests are
    prepared in advance to guide future development.
    """

    def setUp(self):
        """Set up a fresh repository for each test.

        This method creates a new empty repo_path and Repo object
        as provided by the PorcelainTestCase base class.
        """
        super().setUp()

    def _commit_file(self, rel_path, content=b"contents"):
        """Add a file at the given relative path and commit it.

        Creates necessary directories, writes the file content,
        stages, and commits. The commit message and author/committer
        are also provided.
        """
        full_path = os.path.join(self.repo_path, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(content)
        porcelain.add(self.repo_path, paths=[full_path])
        porcelain.commit(
            self.repo_path,
            message=b"Adding " + rel_path.encode("utf-8"),
            author=b"Test Author <author@example.com>",
            committer=b"Test Committer <committer@example.com>",
        )

    def _list_wtree_files(self):
        """Return a set of all file paths relative to the repository root.

        Walks the working tree, skipping the .git directory.
        """
        found_files = set()
        for root, dirs, files in os.walk(self.repo_path):
            if ".git" in dirs:
                dirs.remove(".git")
            for fn in files:
                relp = os.path.relpath(os.path.join(root, fn), self.repo_path)
                found_files.add(relp)
        return found_files

    def test_init_excludes_everything(self):
        """Verify that cone_mode_init writes minimal patterns and empties the working tree.

        Make some dummy files, commit them, then call cone_mode_init. Confirm
        that the working tree is empty, the sparse-checkout file has the
        minimal patterns (/*, !/*/), and the relevant config values are set.
        """
        self._commit_file("docs/readme.md", b"# doc\n")
        self._commit_file("src/main.py", b"print('hello')\n")

        porcelain.cone_mode_init(self.repo)

        actual_files = self._list_wtree_files()
        self.assertEqual(
            set(),
            actual_files,
            "cone_mode_init did not exclude all files from the working tree.",
        )

        sp_path = os.path.join(self.repo_path, ".git", "info", "sparse-checkout")
        with open(sp_path) as f:
            lines = [ln.strip() for ln in f if ln.strip()]

        self.assertIn("/*", lines)
        self.assertIn("!/*/", lines)

        config = self.repo.get_config()
        self.assertEqual(config.get((b"core",), b"sparseCheckout"), b"true")
        self.assertEqual(config.get((b"core",), b"sparseCheckoutCone"), b"true")

    def test_set_specific_dirs(self):
        """Verify that cone_mode_set overwrites the included directories to only the specified ones.

        Initializes cone mode, commits some files, then calls cone_mode_set with
        a list of directories. Expects that only those directories remain in the
        working tree.
        """
        porcelain.cone_mode_init(self.repo)
        self._commit_file("docs/readme.md", b"# doc\n")
        self._commit_file("src/main.py", b"print('hello')\n")
        self._commit_file("tests/test_foo.py", b"# tests\n")

        # Everything is still excluded initially by init.

        porcelain.cone_mode_set(self.repo, dirs=["docs", "src"])

        actual_files = self._list_wtree_files()
        expected_files = {
            os.path.join("docs", "readme.md"),
            os.path.join("src", "main.py"),
        }
        self.assertEqual(
            expected_files,
            actual_files,
            "Did not see only the 'docs/' and 'src/' dirs in the working tree.",
        )

        sp_path = os.path.join(self.repo_path, ".git", "info", "sparse-checkout")
        with open(sp_path) as f:
            lines = [ln.strip() for ln in f if ln.strip()]

        # For standard cone mode, we'd expect lines like:
        #    /*           (include top-level files)
        #    !/*/         (exclude subdirectories)
        #    !/docs/      (re-include docs)
        #    !/src/       (re-include src)
        # Instead of the wildcard-based lines the old test used.
        self.assertIn("/*", lines)
        self.assertIn("!/*/", lines)
        self.assertIn("/docs/", lines)
        self.assertIn("/src/", lines)
        self.assertNotIn("/tests/", lines)

    def test_set_overwrites_old_dirs(self):
        """Ensure that calling cone_mode_set again overwrites old includes.

        Initializes cone mode, includes two directories, then calls
        cone_mode_set again with a different directory to confirm the
        new set of includes replaces the old.
        """
        porcelain.cone_mode_init(self.repo)
        self._commit_file("docs/readme.md")
        self._commit_file("src/main.py")
        self._commit_file("tests/test_bar.py")

        porcelain.cone_mode_set(self.repo, dirs=["docs", "src"])
        self.assertEqual(
            {os.path.join("docs", "readme.md"), os.path.join("src", "main.py")},
            self._list_wtree_files(),
        )

        # Overwrite includes, now only 'tests'
        porcelain.cone_mode_set(self.repo, dirs=["tests"], force=True)

        actual_files = self._list_wtree_files()
        expected_files = {os.path.join("tests", "test_bar.py")}
        self.assertEqual(expected_files, actual_files)

    def test_force_removal_of_local_mods(self):
        """Confirm that force=True removes local changes in excluded paths.

        cone_mode_init and cone_mode_set are called, a file is locally modified,
        and then cone_mode_set is called again with force=True to exclude that path.
        The excluded file should be removed with no CheckoutError.
        """
        porcelain.cone_mode_init(self.repo)
        porcelain.cone_mode_set(self.repo, dirs=["docs"])

        self._commit_file("docs/readme.md", b"Docs stuff\n")
        self._commit_file("src/main.py", b"print('hello')\n")

        # Modify src/main.py
        with open(os.path.join(self.repo_path, "src/main.py"), "ab") as f:
            f.write(b"extra line\n")

        # Exclude src/ with force=True
        porcelain.cone_mode_set(self.repo, dirs=["docs"], force=True)

        actual_files = self._list_wtree_files()
        expected_files = {os.path.join("docs", "readme.md")}
        self.assertEqual(expected_files, actual_files)

    def test_add_and_merge_dirs(self):
        """Verify that cone_mode_add merges new directories instead of overwriting them.

        After initializing cone mode and including a single directory, call
        cone_mode_add with a new directory. Confirm that both directories
        remain included. Repeat for an additional directory to ensure it
        is merged, not overwritten.
        """
        porcelain.cone_mode_init(self.repo)
        self._commit_file("docs/readme.md", b"# doc\n")
        self._commit_file("src/main.py", b"print('hello')\n")
        self._commit_file("tests/test_bar.py", b"# tests\n")

        # Include "docs" only
        porcelain.cone_mode_set(self.repo, dirs=["docs"])
        self.assertEqual({os.path.join("docs", "readme.md")}, self._list_wtree_files())

        # Add "src"
        porcelain.cone_mode_add(self.repo, dirs=["src"])
        actual_files = self._list_wtree_files()
        self.assertEqual(
            {os.path.join("docs", "readme.md"), os.path.join("src", "main.py")},
            actual_files,
        )

        # Add "tests" as well
        porcelain.cone_mode_add(self.repo, dirs=["tests"])
        actual_files = self._list_wtree_files()
        expected_files = {
            os.path.join("docs", "readme.md"),
            os.path.join("src", "main.py"),
            os.path.join("tests", "test_bar.py"),
        }
        self.assertEqual(expected_files, actual_files)

        # Check .git/info/sparse-checkout
        sp_path = os.path.join(self.repo_path, ".git", "info", "sparse-checkout")
        with open(sp_path) as f:
            lines = [ln.strip() for ln in f if ln.strip()]

        # Standard cone mode lines:
        # "/*"    -> include top-level
        # "!/*/"  -> exclude subdirectories
        # "!/docs/", "!/src/", "!/tests/" -> re-include the directories we added
        self.assertIn("/*", lines)
        self.assertIn("!/*/", lines)
        self.assertIn("/docs/", lines)
        self.assertIn("/src/", lines)
        self.assertIn("/tests/", lines)


class UnpackObjectsTest(PorcelainTestCase):
    def test_unpack_objects(self):
        """Test unpacking objects from a pack file."""
        # Create a test repository with some objects
        b1 = Blob()
        b1.data = b"test content 1"
        b2 = Blob()
        b2.data = b"test content 2"

        # Add objects to the repo
        self.repo.object_store.add_object(b1)
        self.repo.object_store.add_object(b2)

        # Create a pack file with these objects
        pack_path = os.path.join(self.test_dir, "test_pack")
        with (
            open(pack_path + ".pack", "wb") as pack_f,
            open(pack_path + ".idx", "wb") as idx_f,
        ):
            porcelain.pack_objects(
                self.repo,
                [b1.id, b2.id],
                pack_f,
                idx_f,
            )

        # Create a new repository to unpack into
        target_repo_path = os.path.join(self.test_dir, "target_repo")
        target_repo = Repo.init(target_repo_path, mkdir=True)
        self.addCleanup(target_repo.close)

        # Unpack the objects
        count = porcelain.unpack_objects(pack_path + ".pack", target_repo_path)

        # Verify the objects were unpacked
        self.assertEqual(2, count)
        self.assertIn(b1.id, target_repo.object_store)
        self.assertIn(b2.id, target_repo.object_store)

        # Verify the content is correct
        unpacked_b1 = target_repo.object_store[b1.id]
        unpacked_b2 = target_repo.object_store[b2.id]
        self.assertEqual(b1.data, unpacked_b1.data)
        self.assertEqual(b2.data, unpacked_b2.data)


class CountObjectsTests(PorcelainTestCase):
    def test_count_objects_empty_repo(self):
        """Test counting objects in an empty repository."""
        stats = porcelain.count_objects(self.repo)
        self.assertEqual(0, stats.count)
        self.assertEqual(0, stats.size)

    def test_count_objects_verbose_empty_repo(self):
        """Test verbose counting in an empty repository."""
        stats = porcelain.count_objects(self.repo, verbose=True)
        self.assertEqual(0, stats.count)
        self.assertEqual(0, stats.size)
        self.assertEqual(0, stats.in_pack)
        self.assertEqual(0, stats.packs)
        self.assertEqual(0, stats.size_pack)

    def test_count_objects_with_loose_objects(self):
        """Test counting loose objects."""
        # Create some loose objects
        blob1 = make_object(Blob, data=b"data1")
        blob2 = make_object(Blob, data=b"data2")
        self.repo.object_store.add_object(blob1)
        self.repo.object_store.add_object(blob2)

        stats = porcelain.count_objects(self.repo)
        self.assertEqual(2, stats.count)
        self.assertGreater(stats.size, 0)

    def test_count_objects_verbose_with_objects(self):
        """Test verbose counting with both loose and packed objects."""
        # Add some loose objects
        for i in range(3):
            blob = make_object(Blob, data=f"data{i}".encode())
            self.repo.object_store.add_object(blob)

        # Create a simple commit to have some objects in a pack
        tree = Tree()
        c1 = make_commit(tree=tree.id, message=b"Test commit")
        self.repo.object_store.add_objects([(tree, None), (c1, None)])
        self.repo.refs[b"HEAD"] = c1.id

        # Repack to create a pack file
        porcelain.repack(self.repo)

        stats = porcelain.count_objects(self.repo, verbose=True)

        # After repacking, loose objects might be cleaned up
        self.assertIsInstance(stats.count, int)
        self.assertIsInstance(stats.size, int)
        self.assertGreater(stats.in_pack, 0)  # Should have packed objects
        self.assertGreater(stats.packs, 0)  # Should have at least one pack
        self.assertGreater(stats.size_pack, 0)  # Pack should have size

        # Verify it's the correct dataclass type
        self.assertIsInstance(stats, CountObjectsResult)


class PruneTests(PorcelainTestCase):
    def test_prune_removes_old_tempfiles(self):
        """Test that prune removes old temporary files."""
        # Create an old temporary file in the objects directory
        objects_dir = os.path.join(self.repo.path, ".git", "objects")
        tmp_pack_path = os.path.join(objects_dir, "tmp_pack_test")
        with open(tmp_pack_path, "wb") as f:
            f.write(b"old temporary data")

        # Make it old
        old_time = time.time() - (DEFAULT_TEMPFILE_GRACE_PERIOD + 3600)
        os.utime(tmp_pack_path, (old_time, old_time))

        # Run prune
        porcelain.prune(self.repo.path)

        # Verify the file was removed
        self.assertFalse(os.path.exists(tmp_pack_path))

    def test_prune_keeps_recent_tempfiles(self):
        """Test that prune keeps recent temporary files."""
        # Create a recent temporary file
        objects_dir = os.path.join(self.repo.path, ".git", "objects")
        tmp_pack_path = os.path.join(objects_dir, "tmp_pack_recent")
        with open(tmp_pack_path, "wb") as f:
            f.write(b"recent temporary data")
        self.addCleanup(os.remove, tmp_pack_path)

        # Run prune
        porcelain.prune(self.repo.path)

        # Verify the file was NOT removed
        self.assertTrue(os.path.exists(tmp_pack_path))

    def test_prune_with_custom_grace_period(self):
        """Test prune with custom grace period."""
        # Create a 1-hour-old temporary file
        objects_dir = os.path.join(self.repo.path, ".git", "objects")
        tmp_pack_path = os.path.join(objects_dir, "tmp_pack_1hour")
        with open(tmp_pack_path, "wb") as f:
            f.write(b"1 hour old data")

        # Make it 1 hour old
        old_time = time.time() - 3600
        os.utime(tmp_pack_path, (old_time, old_time))

        # Prune with 30-minute grace period should remove it
        porcelain.prune(self.repo.path, grace_period=1800)

        # Verify the file was removed
        self.assertFalse(os.path.exists(tmp_pack_path))

    def test_prune_dry_run(self):
        """Test prune in dry-run mode."""
        # Create an old temporary file
        objects_dir = os.path.join(self.repo.path, ".git", "objects")
        tmp_pack_path = os.path.join(objects_dir, "tmp_pack_dryrun")
        with open(tmp_pack_path, "wb") as f:
            f.write(b"old temporary data")
        self.addCleanup(os.remove, tmp_pack_path)

        # Make it old
        old_time = time.time() - (DEFAULT_TEMPFILE_GRACE_PERIOD + 3600)
        os.utime(tmp_pack_path, (old_time, old_time))

        # Run prune in dry-run mode
        porcelain.prune(self.repo.path, dry_run=True)

        # Verify the file was NOT removed (dry run)
        self.assertTrue(os.path.exists(tmp_pack_path))


class FilterBranchTests(PorcelainTestCase):
    def setUp(self):
        super().setUp()
        # Create initial commits with different authors
        from dulwich.objects import Commit, Tree

        # Create actual tree and blob objects
        tree = Tree()
        self.repo.object_store.add_object(tree)

        c1 = Commit()
        c1.tree = tree.id
        c1.parents = []
        c1.author = b"Old Author <old@example.com>"
        c1.author_time = 1000
        c1.author_timezone = 0
        c1.committer = b"Old Committer <old@example.com>"
        c1.commit_time = 1000
        c1.commit_timezone = 0
        c1.message = b"Initial commit"
        self.repo.object_store.add_object(c1)

        c2 = Commit()
        c2.tree = tree.id
        c2.parents = [c1.id]
        c2.author = b"Another Author <another@example.com>"
        c2.author_time = 2000
        c2.author_timezone = 0
        c2.committer = b"Another Committer <another@example.com>"
        c2.commit_time = 2000
        c2.commit_timezone = 0
        c2.message = b"Second commit\n\nWith body"
        self.repo.object_store.add_object(c2)

        c3 = Commit()
        c3.tree = tree.id
        c3.parents = [c2.id]
        c3.author = b"Third Author <third@example.com>"
        c3.author_time = 3000
        c3.author_timezone = 0
        c3.committer = b"Third Committer <third@example.com>"
        c3.commit_time = 3000
        c3.commit_timezone = 0
        c3.message = b"Third commit"
        self.repo.object_store.add_object(c3)

        self.repo.refs[b"refs/heads/master"] = c3.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        # Store IDs for test assertions
        self.c1_id = c1.id
        self.c2_id = c2.id
        self.c3_id = c3.id

    def test_filter_branch_author(self):
        """Test filtering branch with author changes."""

        def filter_author(author):
            # Change all authors to "New Author"
            return b"New Author <new@example.com>"

        result = porcelain.filter_branch(
            self.repo_path, "master", filter_author=filter_author
        )

        # Check that we have mappings for all commits
        self.assertEqual(len(result), 3)

        # Verify the branch ref was updated
        new_head = self.repo.refs[b"refs/heads/master"]
        self.assertNotEqual(new_head, self.c3_id)

        # Verify the original ref was saved
        original_ref = self.repo.refs[b"refs/original/refs/heads/master"]
        self.assertEqual(original_ref, self.c3_id)

        # Check that authors were updated
        new_commit = self.repo[new_head]
        self.assertEqual(new_commit.author, b"New Author <new@example.com>")

        # Check parent chain
        parent = self.repo[new_commit.parents[0]]
        self.assertEqual(parent.author, b"New Author <new@example.com>")

    def test_filter_branch_message(self):
        """Test filtering branch with message changes."""

        def filter_message(message):
            # Add prefix to all messages
            return b"[FILTERED] " + message

        porcelain.filter_branch(self.repo_path, "master", filter_message=filter_message)

        # Verify messages were updated
        new_head = self.repo.refs[b"refs/heads/master"]
        new_commit = self.repo[new_head]
        self.assertTrue(new_commit.message.startswith(b"[FILTERED] "))

    def test_filter_branch_custom_filter(self):
        """Test filtering branch with custom filter function."""

        def custom_filter(commit):
            # Change both author and message
            return {
                "author": b"Custom Author <custom@example.com>",
                "message": b"Custom: " + commit.message,
            }

        porcelain.filter_branch(self.repo_path, "master", filter_fn=custom_filter)

        # Verify custom filter was applied
        new_head = self.repo.refs[b"refs/heads/master"]
        new_commit = self.repo[new_head]
        self.assertEqual(new_commit.author, b"Custom Author <custom@example.com>")
        self.assertTrue(new_commit.message.startswith(b"Custom: "))

    def test_filter_branch_no_changes(self):
        """Test filtering branch with no changes."""
        result = porcelain.filter_branch(self.repo_path, "master")

        # All commits should map to themselves
        for old_sha, new_sha in result.items():
            self.assertEqual(old_sha, new_sha)

        # HEAD should be unchanged
        self.assertEqual(self.repo.refs[b"refs/heads/master"], self.c3_id)

    def test_filter_branch_force(self):
        """Test force filtering a previously filtered branch."""
        # First filter
        porcelain.filter_branch(
            self.repo_path, "master", filter_message=lambda m: b"First: " + m
        )

        # Try again without force - should fail
        with self.assertRaises(porcelain.Error):
            porcelain.filter_branch(
                self.repo_path, "master", filter_message=lambda m: b"Second: " + m
            )

        # Try again with force - should succeed
        porcelain.filter_branch(
            self.repo_path,
            "master",
            filter_message=lambda m: b"Second: " + m,
            force=True,
        )

        # Verify second filter was applied
        new_head = self.repo.refs[b"refs/heads/master"]
        new_commit = self.repo[new_head]
        self.assertTrue(new_commit.message.startswith(b"Second: First: "))


class StashTests(PorcelainTestCase):
    def setUp(self) -> None:
        super().setUp()
        # Create initial commit
        with open(os.path.join(self.repo.path, "initial.txt"), "wb") as f:
            f.write(b"initial content")
        porcelain.add(repo=self.repo.path, paths=["initial.txt"])
        porcelain.commit(
            repo=self.repo.path,
            message=b"Initial commit",
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
        )

    def test_stash_push_and_pop(self) -> None:
        # Create a new file and stage it
        new_file = os.path.join(self.repo.path, "new.txt")
        with open(new_file, "wb") as f:
            f.write(b"new file content")
        porcelain.add(repo=self.repo.path, paths=["new.txt"])

        # Modify existing file
        with open(os.path.join(self.repo.path, "initial.txt"), "wb") as f:
            f.write(b"modified content")

        # Push to stash
        porcelain.stash_push(self.repo.path)

        # Verify files are reset
        self.assertFalse(os.path.exists(new_file))
        with open(os.path.join(self.repo.path, "initial.txt"), "rb") as f:
            self.assertEqual(b"initial content", f.read())

        # Pop the stash
        porcelain.stash_pop(self.repo.path)

        # Verify files are restored
        self.assertTrue(os.path.exists(new_file))
        with open(new_file, "rb") as f:
            self.assertEqual(b"new file content", f.read())
        with open(os.path.join(self.repo.path, "initial.txt"), "rb") as f:
            self.assertEqual(b"modified content", f.read())

        # Verify new file is in the index
        from dulwich.index import Index

        index = Index(os.path.join(self.repo.path, ".git", "index"))
        self.assertIn(b"new.txt", index)

    def test_stash_list(self) -> None:
        # Initially no stashes
        stashes = list(porcelain.stash_list(self.repo.path))
        self.assertEqual(0, len(stashes))

        # Create a file and stash it
        test_file = os.path.join(self.repo.path, "test.txt")
        with open(test_file, "wb") as f:
            f.write(b"test content")
        porcelain.add(repo=self.repo.path, paths=["test.txt"])

        # Push first stash
        porcelain.stash_push(self.repo.path)

        # Create another file and stash it
        test_file2 = os.path.join(self.repo.path, "test2.txt")
        with open(test_file2, "wb") as f:
            f.write(b"test content 2")
        porcelain.add(repo=self.repo.path, paths=["test2.txt"])

        # Push second stash
        porcelain.stash_push(self.repo.path)

        # Check stash list
        stashes = list(porcelain.stash_list(self.repo.path))
        self.assertEqual(2, len(stashes))

        # Stashes are returned in order (most recent first)
        self.assertEqual(0, stashes[0][0])
        self.assertEqual(1, stashes[1][0])

    def test_stash_drop(self) -> None:
        # Create and stash some changes
        test_file = os.path.join(self.repo.path, "test.txt")
        with open(test_file, "wb") as f:
            f.write(b"test content")
        porcelain.add(repo=self.repo.path, paths=["test.txt"])
        porcelain.stash_push(self.repo.path)

        # Create another stash
        test_file2 = os.path.join(self.repo.path, "test2.txt")
        with open(test_file2, "wb") as f:
            f.write(b"test content 2")
        porcelain.add(repo=self.repo.path, paths=["test2.txt"])
        porcelain.stash_push(self.repo.path)

        # Verify we have 2 stashes
        stashes = list(porcelain.stash_list(self.repo.path))
        self.assertEqual(2, len(stashes))

        # Drop the first stash (index 0)
        porcelain.stash_drop(self.repo.path, 0)

        # Verify we have 1 stash left
        stashes = list(porcelain.stash_list(self.repo.path))
        self.assertEqual(1, len(stashes))

        # The remaining stash should be the one we created first
        # Pop it and verify it's the first file
        porcelain.stash_pop(self.repo.path)
        self.assertTrue(os.path.exists(test_file))
        self.assertFalse(os.path.exists(test_file2))

    def test_stash_pop_empty(self) -> None:
        # Attempting to pop from empty stash should raise an error
        with self.assertRaises(IndexError):
            porcelain.stash_pop(self.repo.path)

    def test_stash_with_untracked_files(self) -> None:
        # Create an untracked file
        untracked_file = os.path.join(self.repo.path, "untracked.txt")
        with open(untracked_file, "wb") as f:
            f.write(b"untracked content")

        # Create a tracked change
        tracked_file = os.path.join(self.repo.path, "tracked.txt")
        with open(tracked_file, "wb") as f:
            f.write(b"tracked content")
        porcelain.add(repo=self.repo.path, paths=["tracked.txt"])

        # Stash (by default, untracked files are not included)
        porcelain.stash_push(self.repo.path)

        # Untracked file should still exist
        self.assertTrue(os.path.exists(untracked_file))
        # Tracked file should be gone
        self.assertFalse(os.path.exists(tracked_file))

        # Pop the stash
        porcelain.stash_pop(self.repo.path)

        # Tracked file should be restored
        self.assertTrue(os.path.exists(tracked_file))


class BisectTests(PorcelainTestCase):
    """Tests for bisect porcelain functions."""

    def test_bisect_start(self):
        """Test starting a bisect session."""
        # Create some commits
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store,
            [[1], [2, 1], [3, 2]],
            attrs={
                1: {"message": b"initial"},
                2: {"message": b"second"},
                3: {"message": b"third"},
            },
        )
        self.repo.refs[b"refs/heads/master"] = c3.id
        self.repo.refs[b"HEAD"] = c3.id

        # Start bisect
        porcelain.bisect_start(self.repo_path)

        # Check that bisect state files exist
        self.assertTrue(
            os.path.exists(os.path.join(self.repo.controldir(), "BISECT_START"))
        )
        self.assertTrue(
            os.path.exists(os.path.join(self.repo.controldir(), "BISECT_TERMS"))
        )
        self.assertTrue(
            os.path.exists(os.path.join(self.repo.controldir(), "BISECT_NAMES"))
        )
        self.assertTrue(
            os.path.exists(os.path.join(self.repo.controldir(), "BISECT_LOG"))
        )

    def test_bisect_workflow(self):
        """Test a complete bisect workflow."""
        # Create some commits
        c1, c2, c3, c4 = build_commit_graph(
            self.repo.object_store,
            [[1], [2, 1], [3, 2], [4, 3]],
            attrs={
                1: {"message": b"good commit 1"},
                2: {"message": b"good commit 2"},
                3: {"message": b"bad commit"},
                4: {"message": b"bad commit 2"},
            },
        )
        self.repo.refs[b"refs/heads/master"] = c4.id
        self.repo.refs[b"HEAD"] = c4.id

        # Start bisect with bad and good
        next_sha = porcelain.bisect_start(self.repo_path, bad=c4.id, good=c1.id)

        # Should return the middle commit
        self.assertIsNotNone(next_sha)
        self.assertIn(next_sha, [c2.id, c3.id])

        # Mark the middle commit as good or bad
        if next_sha == c2.id:
            # c2 is good, next should be c3
            next_sha = porcelain.bisect_good(self.repo_path)
            self.assertEqual(next_sha, c3.id)
            # Mark c3 as bad - bisect complete
            next_sha = porcelain.bisect_bad(self.repo_path)
            self.assertIsNone(next_sha)
        else:
            # c3 is bad, next should be c2
            next_sha = porcelain.bisect_bad(self.repo_path)
            self.assertEqual(next_sha, c2.id)
            # Mark c2 as good - bisect complete
            next_sha = porcelain.bisect_good(self.repo_path)
            self.assertIsNone(next_sha)

    def test_bisect_log(self):
        """Test getting bisect log."""
        # Create some commits
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store,
            [[1], [2, 1], [3, 2]],
            attrs={
                1: {"message": b"initial"},
                2: {"message": b"second"},
                3: {"message": b"third"},
            },
        )
        self.repo.refs[b"refs/heads/master"] = c3.id
        self.repo.refs[b"HEAD"] = c3.id

        # Start bisect and mark commits
        porcelain.bisect_start(self.repo_path)
        porcelain.bisect_bad(self.repo_path, c3.id)
        porcelain.bisect_good(self.repo_path, c1.id)

        # Get log
        log = porcelain.bisect_log(self.repo_path)

        self.assertIn("git bisect start", log)
        self.assertIn("git bisect bad", log)
        self.assertIn("git bisect good", log)

    def test_bisect_reset(self):
        """Test resetting bisect state."""
        # Create some commits
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store,
            [[1], [2, 1], [3, 2]],
            attrs={
                1: {"message": b"initial"},
                2: {"message": b"second"},
                3: {"message": b"third"},
            },
        )
        self.repo.refs[b"refs/heads/master"] = c3.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        # Start bisect
        porcelain.bisect_start(self.repo_path)
        porcelain.bisect_bad(self.repo_path)
        porcelain.bisect_good(self.repo_path, c1.id)

        # Reset
        porcelain.bisect_reset(self.repo_path)

        # Check that bisect state files are removed
        self.assertFalse(
            os.path.exists(os.path.join(self.repo.controldir(), "BISECT_START"))
        )
        self.assertFalse(
            os.path.exists(os.path.join(self.repo.controldir(), "refs", "bisect"))
        )

        # HEAD should be back to being a symbolic ref to master
        head_target, _ = self.repo.refs.follow(b"HEAD")
        self.assertEqual(head_target[-1], b"refs/heads/master")

    def test_bisect_skip(self):
        """Test skipping commits during bisect."""
        # Create some commits
        c1, c2, c3, c4, c5 = build_commit_graph(
            self.repo.object_store,
            [[1], [2, 1], [3, 2], [4, 3], [5, 4]],
            attrs={
                1: {"message": b"good"},
                2: {"message": b"skip this"},
                3: {"message": b"bad"},
                4: {"message": b"bad"},
                5: {"message": b"bad"},
            },
        )
        self.repo.refs[b"refs/heads/master"] = c5.id
        self.repo.refs[b"HEAD"] = c5.id

        # Start bisect
        porcelain.bisect_start(self.repo_path, bad=c5.id, good=c1.id)

        # Skip c2 if it's selected
        next_sha = porcelain.bisect_skip(self.repo_path, [c2.id])
        self.assertIsNotNone(next_sha)
        self.assertNotEqual(next_sha, c2.id)
