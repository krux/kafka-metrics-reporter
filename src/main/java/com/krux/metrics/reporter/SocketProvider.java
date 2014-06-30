package com.krux.metrics.reporter;

import java.net.Socket;

public interface SocketProvider {
    Socket get() throws Exception;
}
