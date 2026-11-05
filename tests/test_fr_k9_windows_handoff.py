"""Windows shared sockets over the authenticated control pipe (FR-K9, PR-5)."""
import base64
import os

import pytest

from darkpyonix._client import KernelClient
from darkpyonix._events import EventLog
from darkpyonix._protocol import DKPError
from darkpyonix._server import ControlServer
from helpers import handoff as h

pytestmark = pytest.mark.skipif(os.name != "nt", reason="requires Windows socket sharing")
KID = "k_0123456789abcdef0123"


def test_fr_k9_adopted_socket_on_windows(dp_home):
    srv = ControlServer(KID, EventLog(), {})
    srv.start()
    client = KernelClient(srv.address, KID)
    client.connect()
    peer, handed = h.tcp_pair()
    try:
        result = client.request("adopt", {
            "kind": "events", "transport": "share",
            "share": base64.b64encode(handed.share(os.getpid())).decode("ascii"),
            "request": base64.b64encode(h.http_get("/api/kernels/%s/events" % KID)).decode("ascii"),
            "label": {"capabilities": ["read", "history"]},
        })
        handed.close()
        assert result["stream_id"].startswith("st_")
        assert h.read_head(peer)[0].startswith("HTTP/1.1 200")
        with pytest.raises(DKPError) as exc:
            client.request("adopt", {"transport": "fd"})
        assert exc.value.code == "bad_request"
        with pytest.raises(DKPError) as exc:
            client.request("adopt", {"transport": "share", "share": "invalid!"})
        assert exc.value.code == "bad_request"
    finally:
        handed.close()
        peer.close()
        client.close()
        srv.stop()
